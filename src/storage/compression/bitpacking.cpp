#include "duckdb/common/bitpacking.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/compression/bitpacking.hpp"
#include "duckdb/storage/compression/standard_compression_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <functional>
#include <type_traits>

namespace duckdb {

constexpr const idx_t BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
static constexpr const idx_t BITPACKING_METADATA_GROUP_SIZE = STANDARD_VECTOR_SIZE > 512 ? STANDARD_VECTOR_SIZE : 2048;

typedef struct {
	BitpackingMode mode;
	uint32_t offset;
} bitpacking_metadata_t;

typedef uint32_t bitpacking_metadata_encoded_t;

static bitpacking_metadata_encoded_t EncodeMeta(bitpacking_metadata_t metadata) {
	D_ASSERT(metadata.offset <= 0x00FFFFFF); // max uint24_t
	bitpacking_metadata_encoded_t encoded_value = metadata.offset;
	encoded_value |= UnsafeNumericCast<bitpacking_metadata_encoded_t>((uint8_t)metadata.mode << 24);
	return encoded_value;
}
static bitpacking_metadata_t DecodeMeta(bitpacking_metadata_encoded_t *metadata_encoded) {
	bitpacking_metadata_t metadata;
	metadata.mode = static_cast<BitpackingMode>((*metadata_encoded >> 24) & 0xFF);
	metadata.offset = *metadata_encoded & 0x00FFFFFF;
	return metadata;
}

struct EmptyBitpackingWriter {
	template <class T>
	static void WriteConstant(T constant, idx_t count, void *data_ptr, bool all_invalid) {
	}
	template <class T, class T_S = typename MakeSigned<T>::type>
	static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T *values, bool *validity,
	                               void *data_ptr) {
	}
	template <class T, class T_S = typename MakeSigned<T>::type>
	static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference,
	                          T_S delta_offset, T *original_values, idx_t count, void *data_ptr) {
	}
	template <class T>
	static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count,
	                     void *data_ptr) {
	}
};

template <class T, class T_S = typename MakeSigned<T>::type>
struct BitpackingState {
public:
	BitpackingState() : compression_buffer_idx(0), total_size(0), data_ptr(nullptr) {
		compression_buffer_internal[0] = T(0);
		compression_buffer = &compression_buffer_internal[1];
		Reset();
	}

	// Extra val for delta encoding
	T compression_buffer_internal[BITPACKING_METADATA_GROUP_SIZE + 1];
	T *compression_buffer;
	T_S delta_buffer[BITPACKING_METADATA_GROUP_SIZE];
	bool compression_buffer_validity[BITPACKING_METADATA_GROUP_SIZE];
	idx_t compression_buffer_idx;
	idx_t total_size;

	// Used to pass CompressionState ptr through the Bitpacking writer
	void *data_ptr;

	// Stats on current compression buffer
	T minimum;
	T maximum;
	T min_max_diff;
	T_S minimum_delta;
	T_S maximum_delta;
	T_S min_max_delta_diff;
	T_S delta_offset;
	bool all_valid;
	bool all_invalid;

	bool has_valid;
	bool has_invalid;

	bool can_do_delta;
	bool can_do_for;

	// Used to force a specific mode, useful in testing
	BitpackingMode mode = BitpackingMode::AUTO;

public:
	void Reset() {
		minimum = NumericLimits<T>::Maximum();
		minimum_delta = NumericLimits<T_S>::Maximum();
		maximum = NumericLimits<T>::Minimum();
		maximum_delta = NumericLimits<T_S>::Minimum();
		delta_offset = 0;
		all_valid = true;
		all_invalid = true;
		has_valid = false;
		has_invalid = false;
		can_do_delta = false;
		can_do_for = false;
		compression_buffer_idx = 0;
		min_max_diff = 0;
		min_max_delta_diff = 0;
	}

	void CalculateFORStats() {
		can_do_for = TrySubtractOperator::Operation(maximum, minimum, min_max_diff);
	}

	void CalculateDeltaStats() {
		// TODO: currently we dont support delta compression of values above NumericLimits<T_S>::Maximum(),
		// 		 we could support this with some clever subtract trickery?
		if (maximum > static_cast<T>(NumericLimits<T_S>::Maximum())) {
			return;
		}

		// Don't delta encoding 1 value makes no sense
		if (compression_buffer_idx < 2) {
			return;
		}

		// TODO: handle NULLS here?
		// Currently we cannot handle nulls because we would need an additional step of patching for this.
		// we could for example copy the last value on a null insert. This would help a bit, but not be optimal for
		// large deltas since theres suddenly a zero then. Ideally we would insert a value that leads to a delta within
		// the current domain of deltas however we dont know that domain here yet
		if (!all_valid) {
			return;
		}

		// Note: since we dont allow any values over NumericLimits<T_S>::Maximum(), all subtractions for unsigned types
		// are guaranteed not to overflow
		bool can_do_all = true;
		if (NumericLimits<T>::IsSigned()) {
			T_S bogus;
			can_do_all = TrySubtractOperator::Operation(static_cast<T_S>(minimum), static_cast<T_S>(maximum), bogus) &&
			             TrySubtractOperator::Operation(static_cast<T_S>(maximum), static_cast<T_S>(minimum), bogus);
		}

		// Calculate delta's
		// compression_buffer pointer points one element ahead of the internal buffer making the use of signed index
		// integer (-1) possible
		D_ASSERT(compression_buffer_idx <= NumericLimits<int64_t>::Maximum());
		if (can_do_all) {
			for (int64_t i = 0; i < static_cast<int64_t>(compression_buffer_idx); i++) {
				delta_buffer[i] = static_cast<T_S>(compression_buffer[i]) - static_cast<T_S>(compression_buffer[i - 1]);
			}
		} else {
			for (int64_t i = 0; i < static_cast<int64_t>(compression_buffer_idx); i++) {
				auto success =
				    TrySubtractOperator::Operation(static_cast<T_S>(compression_buffer[i]),
				                                   static_cast<T_S>(compression_buffer[i - 1]), delta_buffer[i]);
				if (!success) {
					return;
				}
			}
		}

		can_do_delta = true;

		for (idx_t i = 1; i < compression_buffer_idx; i++) {
			maximum_delta = MaxValue<T_S>(maximum_delta, delta_buffer[i]);
			minimum_delta = MinValue<T_S>(minimum_delta, delta_buffer[i]);
		}

		// Since we can set the first value arbitrarily, we want to pick one from the current domain, note that
		// we will store the original first value - this offset as the  delta_offset to be able to decode this again.
		delta_buffer[0] = minimum_delta;

		can_do_delta = can_do_delta && TrySubtractOperator::Operation(maximum_delta, minimum_delta, min_max_delta_diff);
		can_do_delta = can_do_delta && TrySubtractOperator::Operation(static_cast<T_S>(compression_buffer[0]),
		                                                              minimum_delta, delta_offset);
	}

	template <class T_INNER>
	void SubtractFrameOfReference(T_INNER *buffer, T_INNER frame_of_reference) {
		static_assert(NumericLimits<T_INNER>::IsIntegral(), "Integral type required.");

		using T_U = typename MakeUnsigned<T_INNER>::type;

		for (idx_t i = 0; i < compression_buffer_idx; i++) {
			reinterpret_cast<T_U *>(buffer)[i] -= static_cast<T_U>(frame_of_reference);
		}
	}

	template <class OP>
	bool Flush() {
		if (compression_buffer_idx == 0) {
			return true;
		}

		if ((all_invalid || maximum == minimum) && (mode == BitpackingMode::AUTO || mode == BitpackingMode::CONSTANT)) {
			OP::WriteConstant(maximum, compression_buffer_idx, data_ptr, all_invalid);
			total_size += sizeof(T) + sizeof(bitpacking_metadata_encoded_t);
			return true;
		}

		CalculateFORStats();
		CalculateDeltaStats();

		if (can_do_delta) {
			if (maximum_delta == minimum_delta && mode != BitpackingMode::FOR && mode != BitpackingMode::DELTA_FOR) {
				// FOR needs to be T (considering hugeint is bigger than idx_t)
				T frame_of_reference = compression_buffer[0];

				OP::WriteConstantDelta(maximum_delta, static_cast<T>(frame_of_reference), compression_buffer_idx,
				                       compression_buffer, compression_buffer_validity, data_ptr);
				total_size += sizeof(T) + sizeof(T) + sizeof(bitpacking_metadata_encoded_t);
				return true;
			}

			// Check if delta has benefit
			auto delta_required_bitwidth =
			    BitpackingPrimitives::MinimumBitWidth<T, false>(static_cast<T>(min_max_delta_diff));
			auto regular_required_bitwidth = BitpackingPrimitives::MinimumBitWidth(min_max_diff);

			//! `min_max_diff` is uninitialized if `can_do_for` isn't true
			bool prefer_for = can_do_for && delta_required_bitwidth >= regular_required_bitwidth;

			if (!prefer_for && mode != BitpackingMode::FOR) {
				SubtractFrameOfReference(delta_buffer, minimum_delta);

				OP::WriteDeltaFor(reinterpret_cast<T *>(delta_buffer), compression_buffer_validity,
				                  delta_required_bitwidth, static_cast<T>(minimum_delta), delta_offset,
				                  compression_buffer, compression_buffer_idx, data_ptr);

				// FOR (frame of reference).
				total_size += sizeof(T);
				// Aligned bitpacking width.
				total_size += AlignValue(sizeof(bitpacking_width_t));
				// Delta offset.
				total_size += sizeof(T);
				// Compressed data size.
				total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, delta_required_bitwidth);

				return true;
			}
		}

		if (can_do_for) {
			auto width = BitpackingPrimitives::MinimumBitWidth<T, false>(min_max_diff);
			SubtractFrameOfReference(compression_buffer, minimum);
			OP::WriteFor(compression_buffer, compression_buffer_validity, width, minimum, compression_buffer_idx,
			             data_ptr);

			total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, width);
			total_size += sizeof(T); // FOR value
			total_size += AlignValue(sizeof(bitpacking_width_t));

			return true;
		}

		return false;
	}

	template <class OP = EmptyBitpackingWriter>
	bool Update(typename VectorIterator<T>::ValueEntry val) {
		auto is_valid = val.IsValid();
		compression_buffer_validity[compression_buffer_idx] = is_valid;
		has_valid = has_valid || is_valid;
		has_invalid = has_invalid || !is_valid;
		all_valid = all_valid && is_valid;
		all_invalid = all_invalid && !is_valid;

		if (is_valid) {
			auto value = val.GetValue();
			compression_buffer[compression_buffer_idx] = value;
			minimum = MinValue<T>(minimum, value);
			maximum = MaxValue<T>(maximum, value);
		}

		compression_buffer_idx++;

		if (compression_buffer_idx == BITPACKING_METADATA_GROUP_SIZE) {
			bool success = Flush<OP>();
			Reset();
			return success;
		}
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingAnalyzeState : public AnalyzeState {
	explicit BitpackingAnalyzeState(BlockManager &block_manager) : AnalyzeState(block_manager) {};
	BitpackingState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto state = make_uniq<BitpackingAnalyzeState<T>>(col_data.GetBlockManager());
	state->state.mode = Settings::Get<ForceBitpackingModeSetting>(col_data.GetDatabase());

	return std::move(state);
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, const Vector &input) {
	// We use BITPACKING_METADATA_GROUP_SIZE tuples, which can exceed the block size.
	// In that case, we disable bitpacking.
	// we are conservative here by multiplying by 2
	auto type_size = GetTypeIdSize(input.GetType().InternalType());
	if (type_size * BITPACKING_METADATA_GROUP_SIZE * 2 > state.info.GetBlockSize()) {
		return false;
	}

	auto &analyze_state = state.Cast<BitpackingAnalyzeState<T>>();
	for (auto entry : input.Values<T>()) {
		if (!analyze_state.state.template Update<EmptyBitpackingWriter>(entry)) {
			return false;
		}
	}
	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = state.Cast<BitpackingAnalyzeState<T>>();
	auto flush_result = bitpacking_state.state.template Flush<EmptyBitpackingWriter>();
	if (!flush_result) {
		return DConstants::INVALID_INDEX;
	}
	return bitpacking_state.state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS, class T_S = typename MakeSigned<T>::type>
struct BitpackingCompressionState : public StandardCompressionState {
public:
	explicit BitpackingCompressionState(ColumnDataCheckpointData &checkpoint_data)
	    : StandardCompressionState(checkpoint_data, CompressionType::COMPRESSION_BITPACKING) {
		CreateEmptySegment();

		state.data_ptr = reinterpret_cast<void *>(this);
		state.mode = Settings::Get<ForceBitpackingModeSetting>(checkpoint_data.GetDatabase());
	}

	StatsWriter<T> stats_writer;
	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths and frame-of-references (growing downwards).
	data_ptr_t metadata_ptr;

	BitpackingState<T> state;

public:
	struct BitpackingWriter {
		static void WriteConstant(T constant, idx_t count, void *data_ptr, bool all_invalid) {
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}

		static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T *values, bool *validity,
		                               void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, 2 * sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT_DELTA);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}
		static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference,
		                          T_S delta_offset, T *original_values, idx_t count, void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

			auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
			ReserveSpace(state, bp_size + 3 * sizeof(T));

			WriteMetaData(state, BitpackingMode::DELTA_FOR);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, static_cast<T>(width));
			WriteData(state->data_ptr, delta_offset);

			BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			state->data_ptr += bp_size;

			UpdateStats(state, count);
		}

		static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count,
		                     void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

			auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
			ReserveSpace(state, bp_size + 2 * sizeof(T));

			WriteMetaData(state, BitpackingMode::FOR);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, (T)width);

			BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			state->data_ptr += bp_size;

			UpdateStats(state, count);
		}

		template <class T_OUT>
		static void WriteData(data_ptr_t &ptr, T_OUT val) {
			*reinterpret_cast<T_OUT *>(ptr) = val;
			ptr += sizeof(T_OUT);
		}

		static void WriteMetaData(BitpackingCompressionState<T, WRITE_STATISTICS> *state, BitpackingMode mode) {
			bitpacking_metadata_t metadata {mode, (uint32_t)(state->data_ptr - state->handle.GetDataMutable())};
			state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
			Store<bitpacking_metadata_encoded_t>(EncodeMeta(metadata), state->metadata_ptr);
		}

		static void ReserveSpace(BitpackingCompressionState<T, WRITE_STATISTICS> *state, idx_t data_bytes) {
			idx_t meta_bytes = sizeof(bitpacking_metadata_encoded_t);
			state->FlushAndCreateSegmentIfFull(data_bytes, meta_bytes);
			D_ASSERT(state->CanStore(data_bytes, meta_bytes));
		}

		static void UpdateStats(BitpackingCompressionState<T, WRITE_STATISTICS> *state, idx_t count) {
			state->current_segment->count += count;

			if (WRITE_STATISTICS) {
				auto &stats_writer = state->stats_writer;
				if (state->state.has_valid) {
					stats_writer.SetHasValid();
					stats_writer.UpdateMinMax(state->state.minimum);
					stats_writer.UpdateMinMax(state->state.maximum);
				}
				if (state->state.has_invalid) {
					stats_writer.SetHasNull();
				}
			}
		}
	};

	bool CanStore(idx_t data_bytes, idx_t meta_bytes) {
		auto required_data_bytes = AlignValue<idx_t>(UnsafeNumericCast<idx_t>((data_ptr + data_bytes) - data_ptr));
		auto required_meta_bytes = info.GetBlockSize() - UnsafeNumericCast<idx_t>(metadata_ptr - data_ptr) + meta_bytes;

		return required_data_bytes + required_meta_bytes <=
		       info.GetBlockSize() - BitpackingPrimitives::BITPACKING_HEADER_SIZE;
	}

	void CreateEmptySegment() {
		CreateAndPinNewSegment();

		data_ptr = handle.GetDataMutable() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		metadata_ptr = handle.GetDataMutable() + info.GetBlockSize();
	}

	void Append(const Vector &input) {
		for (auto entry : input.Values<T>()) {
			state.template Update<BitpackingWriter>(entry);
		}
	}

	void FlushAndCreateSegmentIfFull(idx_t required_data_bytes, idx_t required_meta_bytes) {
		if (!CanStore(required_data_bytes, required_meta_bytes)) {
			FlushSegment();
			CreateEmptySegment();
		}
	}

	void FlushSegment() {
		auto base_ptr = handle.GetDataMutable();

		// Compact the segment by moving the metadata next to the data.

		idx_t unaligned_offset = NumericCast<idx_t>(data_ptr - base_ptr);
		idx_t metadata_offset = AlignValue(unaligned_offset);
		idx_t metadata_size = NumericCast<idx_t>(base_ptr + info.GetBlockSize() - metadata_ptr);
		idx_t total_segment_size = metadata_offset + metadata_size;

		// Asserting things are still sane here
		if (!CanStore(0, 0)) {
			throw InternalException("Error in bitpacking size calculation");
		}

		if (unaligned_offset != metadata_offset) {
			// zero initialize any padding bits
			memset(base_ptr + unaligned_offset, 0, metadata_offset - unaligned_offset);
		}
		memmove(base_ptr + metadata_offset, metadata_ptr, metadata_size);

		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<idx_t>(metadata_offset + metadata_size, base_ptr);
		FlushCurrentSegment(stats_writer, total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressionState<T, WRITE_STATISTICS, T_S>::BitpackingWriter>();
		FlushSegment();
		current_segment.reset();
	}
};

template <class T, bool WRITE_STATISTICS>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                       unique_ptr<AnalyzeState> state) {
	return make_uniq<BitpackingCompressionState<T, WRITE_STATISTICS>>(checkpoint_data);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingCompress(CompressionState &state_p, const Vector &scan_vector) {
	auto &state = state_p.Cast<BitpackingCompressionState<T, WRITE_STATISTICS>>();
	state.Append(scan_vector);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<BitpackingCompressionState<T, WRITE_STATISTICS>>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
static void ApplyFrameOfReference(T *dst, T frame_of_reference, idx_t size) {
	using T_U = typename MakeUnsigned<T>::type;
	if (!frame_of_reference) {
		return;
	}

	for (idx_t i = 0; i < size; i++) {
		reinterpret_cast<T_U *>(dst)[i] += static_cast<T_U>(frame_of_reference);
	}
}

// Based on https://github.com/lemire/FastPFor (Apache License 2.0)
template <class T>
static T DeltaDecode(T *data, T previous_value, const size_t size) {
	D_ASSERT(size >= 1);

	data[0] += previous_value;

	const size_t UnrollQty = 4;
	const size_t sz0 = (size / UnrollQty) * UnrollQty; // equal to 0, if size < UnrollQty
	size_t i = 1;
	if (sz0 >= UnrollQty) {
		T a = data[0];
		for (; i < sz0 - UnrollQty; i += UnrollQty) {
			a = data[i] += a;
			a = data[i + 1] += a;
			a = data[i + 2] += a;
			a = data[i + 3] += a;
		}
	}
	for (; i != size; ++i) {
		data[i] += data[i - 1];
	}

	return data[size - 1];
}

template <class T, class T_S = typename MakeSigned<T>::type>
struct BitpackingScanState : public SegmentScanState {
public:
	explicit BitpackingScanState(const QueryContext &context, ColumnSegment &segment) : current_segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
		handle = buffer_manager.Pin(context, segment.GetBlockHandle());
		auto data_ptr = handle.GetDataMutable();

		// load offset to bitpacking widths pointer
		auto bitpacking_metadata_offset = Load<idx_t>(data_ptr + segment.GetBlockOffset());
		bitpacking_metadata_ptr =
		    data_ptr + segment.GetBlockOffset() + bitpacking_metadata_offset - sizeof(bitpacking_metadata_encoded_t);
		if (bitpacking_metadata_ptr >= handle.GetDataMutable() + current_segment.GetBlockSize()) {
			throw InternalException("Bitpacking offset is out of range at block \"%llu\" - corrupt database file",
			                        segment.GetBlockHandle()->BlockId());
		}

		// load the first group
		LoadNextGroup();
	}

	BufferHandle handle;
	ColumnSegment &current_segment;

	T decompression_buffer[BITPACKING_METADATA_GROUP_SIZE];
	SelectionVector filter_sel;

	bitpacking_metadata_t current_group;
	bitpacking_width_t current_width;
	T current_frame_of_reference;
	T current_constant;
	T current_delta_offset;

	idx_t current_group_offset = 0;
	data_ptr_t current_group_ptr;
	data_ptr_t bitpacking_metadata_ptr;

public:
	//! Loads the metadata for the current metadata group. This will set bitpacking_metadata_ptr to the next group.
	//! It also loads any metadata at the start of a compressed buffer (e.g. the width, for, or constant value)
	//! depending on the bitpacking mode of that group.
	void LoadNextGroup() {
		D_ASSERT(bitpacking_metadata_ptr > handle.GetDataMutable() &&
		         (bitpacking_metadata_ptr < handle.GetDataMutable() + current_segment.GetBlockSize()));
		current_group_offset = 0;
		current_group = DecodeMeta(reinterpret_cast<bitpacking_metadata_encoded_t *>(bitpacking_metadata_ptr));

		bitpacking_metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
		current_group_ptr = GetPtr(current_group);

		// Read first value
		switch (current_group.mode) {
		case BitpackingMode::CONSTANT:
			current_constant = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::FOR:
		case BitpackingMode::CONSTANT_DELTA:
		case BitpackingMode::DELTA_FOR:
			current_frame_of_reference = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		default:
			throw InternalException("Invalid bitpacking mode");
		}

		// Read second value
		switch (current_group.mode) {
		case BitpackingMode::CONSTANT_DELTA:
			current_constant = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::FOR:
		case BitpackingMode::DELTA_FOR:
			current_width = (bitpacking_width_t)(*reinterpret_cast<T *>(current_group_ptr));
			current_group_ptr += MaxValue(sizeof(T), sizeof(bitpacking_width_t));
			break;
		case BitpackingMode::CONSTANT:
			break;
		default:
			throw InternalException("Invalid bitpacking mode");
		}

		// Read third value
		if (current_group.mode == BitpackingMode::DELTA_FOR) {
			current_delta_offset = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
		}
	}

	void SkipInternal(ColumnSegment &segment, idx_t skip_count, bool allow_group_boundary) {
		bool skip_sign_extend = true;

		idx_t skipped = 0;
		idx_t initial_group_offset = current_group_offset;

		// This skips straight to the correct metadata group
		const auto target_group_offset = skip_count + current_group_offset;
		idx_t meta_groups_to_skip = target_group_offset / BITPACKING_METADATA_GROUP_SIZE;
		if (allow_group_boundary && meta_groups_to_skip && target_group_offset % BITPACKING_METADATA_GROUP_SIZE == 0) {
			meta_groups_to_skip--;
		}
		if (meta_groups_to_skip) {
			// bitpacking_metadata_ptr points to the next metadata: this means we need to advance the pointer by n-1
			bitpacking_metadata_ptr -= (meta_groups_to_skip - 1) * sizeof(bitpacking_metadata_encoded_t);
			LoadNextGroup();
			// The first (partial) group we skipped
			skipped += BITPACKING_METADATA_GROUP_SIZE - initial_group_offset;
			// The remaining groups that were skipped
			skipped += (meta_groups_to_skip - 1) * BITPACKING_METADATA_GROUP_SIZE;
		}

		// Assert we can are in the correct metadata group
		idx_t remaining_to_skip = skip_count - skipped;
		if (allow_group_boundary) {
			D_ASSERT(current_group_offset + remaining_to_skip <= BITPACKING_METADATA_GROUP_SIZE);
		} else {
			D_ASSERT(current_group_offset + remaining_to_skip < BITPACKING_METADATA_GROUP_SIZE);
		}

		if (current_group.mode == BitpackingMode::CONSTANT || current_group.mode == BitpackingMode::CONSTANT_DELTA ||
		    current_group.mode == BitpackingMode::FOR) {
			// Skipping within a constant or constant delta is done by increasing the current_group_offset
			skipped += remaining_to_skip;
			current_group_offset += remaining_to_skip;
		} else {
			// For DELTA we actually need to decompress from the current_group_offset up until the row we want to skip
			// to this is because we need that delta to be able to continue scanning from here
			D_ASSERT(current_group.mode == BitpackingMode::DELTA_FOR);

			while (skipped < skip_count) {
				// Calculate compression group offset and pointer
				idx_t offset_in_compression_group =
				    current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
				data_ptr_t current_position_ptr = current_group_ptr + current_group_offset * current_width / 8;
				data_ptr_t decompression_group_start_pointer =
				    current_position_ptr - offset_in_compression_group * current_width / 8;

				idx_t skipping_this_algorithm_group =
				    MinValue(remaining_to_skip,
				             BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);

				BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(decompression_buffer),
				                                     decompression_group_start_pointer, current_width,
				                                     skip_sign_extend);

				T *decompression_ptr = decompression_buffer + offset_in_compression_group;
				ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
				                           static_cast<T_S>(current_frame_of_reference), skipping_this_algorithm_group);
				DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr), static_cast<T_S>(current_delta_offset),
				                 skipping_this_algorithm_group);
				current_delta_offset = decompression_ptr[skipping_this_algorithm_group - 1];

				skipped += skipping_this_algorithm_group;
				current_group_offset += skipping_this_algorithm_group;
				remaining_to_skip -= skipping_this_algorithm_group;
			}
		}

		D_ASSERT(skipped == skip_count);
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		SkipInternal(segment, skip_count, false);
	}

	void Consume(ColumnSegment &segment, idx_t skip_count) {
		SkipInternal(segment, skip_count, true);
	}

	data_ptr_t GetPtr(bitpacking_metadata_t group) {
		return handle.GetDataMutable() + current_segment.GetBlockOffset() + group.offset;
	}
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<BitpackingScanState<T>>(context, segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T, class T_S = typename MakeSigned<T>::type, class T_U = typename MakeUnsigned<T>::type>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();

	T *result_data = FlatVector::GetDataMutable<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);

		// Exhausted this metadata group, move pointers to next group and load metadata for next group.
		if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}

		idx_t offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T *begin = result_data + result_offset + scanned;
			T *end = begin + remaining;
			std::fill(begin, end, scan_state.current_constant);
			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T *target_ptr = result_data + result_offset + scanned;

			for (idx_t i = 0; i < to_scan; i++) {
				idx_t multiplier = scan_state.current_group_offset + i;
				// intended static casts to unsigned and back for defined wrapping of integers
				target_ptr[i] = static_cast<T>((static_cast<T_U>(scan_state.current_constant) * multiplier) +
				                               static_cast<T_U>(scan_state.current_frame_of_reference));
			}

			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);
		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		T *current_result_ptr = result_data + result_offset + scanned;

		if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
			// Decompress directly into result vector
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(current_result_ptr), decompression_group_start_pointer,
			                                     scan_state.current_width, skip_sign_extend);
		} else {
			// Decompress compression algorithm to buffer
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
			                                     decompression_group_start_pointer, scan_state.current_width,
			                                     skip_sign_extend);

			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
		}

		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
			                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = current_result_ptr[to_scan - 1];
		} else {
			ApplyFrameOfReference<T>(current_result_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		scanned += to_scan;
		scan_state.current_group_offset += to_scan;
	}
}

template <class T>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	BitpackingScanPartial<T>(segment, state, scan_count, result, 0);
}

template <class T, class T_U = typename MakeUnsigned<T>::type>
static T BitpackingConstantDeltaValue(const BitpackingScanState<T> &scan_state, idx_t group_offset) {
	return static_cast<T>((static_cast<T_U>(scan_state.current_constant) * group_offset) +
	                      static_cast<T_U>(scan_state.current_frame_of_reference));
}

static bool SelectionVectorIsOrdered(const SelectionVector &sel, idx_t sel_count) {
	if (!sel.IsSet()) {
		return true;
	}
	if (sel_count < 2) {
		return true;
	}
	auto previous_idx = sel.get_index(0);
	for (idx_t i = 1; i < sel_count; i++) {
		auto current_idx = sel.get_index(i);
		if (current_idx < previous_idx) {
			return false;
		}
		previous_idx = current_idx;
	}
	return true;
}

template <class T, class T_S = typename MakeSigned<T>::type>
void BitpackingScanSelected(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                            const SelectionVector &sel, idx_t sel_count) {
	auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();
	auto result_data = FlatVector::GetDataMutable<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	bool skip_sign_extend = true;
	idx_t selected_idx = 0;
	idx_t scanned = 0;
	while (scanned < vector_count) {
		if (selected_idx == sel_count) {
			scan_state.Consume(segment, vector_count - scanned);
			return;
		}
		const auto next_selected = sel.get_index(selected_idx);
		D_ASSERT(next_selected >= scanned);
		if (next_selected > scanned) {
			const auto skip_count = next_selected - scanned;
			scan_state.Skip(segment, skip_count);
			scanned += skip_count;
			continue;
		}

		D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);
		if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			for (idx_t i = selected_idx; i < selected_end; i++) {
				result_data[sel.get_index(i)] = scan_state.current_constant;
			}
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			const auto base_offset = scan_state.current_group_offset;
			for (idx_t i = selected_idx; i < selected_end; i++) {
				const auto row_idx = sel.get_index(i);
				result_data[row_idx] = BitpackingConstantDeltaValue<T>(scan_state, base_offset + row_idx - scanned);
			}
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}

		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

		const auto offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		const auto scan_unit_remaining =
		    MinValue<idx_t>(vector_count - scanned,
		                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);
		idx_t selected_end = selected_idx + 1;
		while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
			selected_end++;
		}
		const auto last_selected = sel.get_index(selected_end - 1);
		const auto to_scan = last_selected - scanned + 1;
		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
		                                     decompression_group_start_pointer, scan_state.current_width,
		                                     skip_sign_extend);

		auto decompression_ptr = scan_state.decompression_buffer + offset_in_compression_group;
		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = decompression_ptr[to_scan - 1];
		} else {
			ApplyFrameOfReference<T>(decompression_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		for (idx_t i = selected_idx; i < selected_end; i++) {
			const auto row_idx = sel.get_index(i);
			result_data[row_idx] = decompression_ptr[row_idx - scanned];
		}
		scan_state.current_group_offset += to_scan;
		scanned += to_scan;
		selected_idx = selected_end;
	}
}

static bool ShouldUseBitpackingSelectedFilter(idx_t vector_count, idx_t sel_count) {
	return sel_count < vector_count / 2;
}

template <class T>
static SelectionVector &BitpackingFilterSelection(BitpackingScanState<T> &scan_state, idx_t count) {
	if (scan_state.filter_sel.Capacity() < count) {
		scan_state.filter_sel.Initialize(MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
	}
	return scan_state.filter_sel;
}

template <class T, class T_S = typename MakeSigned<T>::type>
void BitpackingSelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                      const SelectionVector &sel, idx_t sel_count) {
	if (sel_count == 0) {
		auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();
		scan_state.Consume(segment, vector_count);
		FlatVector::SetSize(result, 0);
		return;
	}
	if (!ShouldUseBitpackingSelectedFilter(vector_count, sel_count) || !SelectionVectorIsOrdered(sel, sel_count)) {
		BitpackingScanPartial<T>(segment, state, vector_count, result, 0);
		FlatVector::SetSize(result, count_t(vector_count));
		result.Slice(sel, sel_count);
		return;
	}

	auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();
	auto result_data = FlatVector::GetDataMutable<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	bool skip_sign_extend = true;
	idx_t selected_idx = 0;
	idx_t scanned = 0;
	idx_t output_idx = 0;
	while (scanned < vector_count) {
		if (selected_idx == sel_count) {
			scan_state.Consume(segment, vector_count - scanned);
			break;
		}
		const auto next_selected = sel.get_index(selected_idx);
		D_ASSERT(next_selected >= scanned);
		if (next_selected > scanned) {
			const auto skip_count = next_selected - scanned;
			scan_state.Skip(segment, skip_count);
			scanned += skip_count;
			continue;
		}

		D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);
		if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			for (idx_t i = selected_idx; i < selected_end; i++) {
				result_data[output_idx++] = scan_state.current_constant;
			}
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			const auto base_offset = scan_state.current_group_offset;
			for (idx_t i = selected_idx; i < selected_end; i++) {
				const auto row_idx = sel.get_index(i);
				result_data[output_idx++] =
				    BitpackingConstantDeltaValue<T>(scan_state, base_offset + row_idx - scanned);
			}
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}

		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);
		const auto offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		const auto scan_unit_remaining =
		    MinValue<idx_t>(vector_count - scanned,
		                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);
		idx_t selected_end = selected_idx + 1;
		while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
			selected_end++;
		}
		const auto last_selected = sel.get_index(selected_end - 1);
		const auto to_scan = last_selected - scanned + 1;

		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
		                                     decompression_group_start_pointer, scan_state.current_width,
		                                     skip_sign_extend);

		auto decompression_ptr = scan_state.decompression_buffer + offset_in_compression_group;
		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = decompression_ptr[to_scan - 1];
		} else {
			ApplyFrameOfReference<T>(decompression_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		for (idx_t i = selected_idx; i < selected_end; i++) {
			const auto row_idx = sel.get_index(i);
			result_data[output_idx++] = decompression_ptr[row_idx - scanned];
		}
		scan_state.current_group_offset += to_scan;
		scanned += to_scan;
		selected_idx = selected_end;
	}
	D_ASSERT(output_idx == sel_count);
	FlatVector::SetSize(result, count_t(sel_count));
}

static optional_ptr<const Expression> TryGetBitpackingSelectivityOptionalChild(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function_expr = expr.Cast<BoundFunctionExpression>();
	if (function_expr.Function().GetName() != SelectivityOptionalFilterScalarFun::NAME || !function_expr.BindInfo()) {
		return nullptr;
	}
	auto &data = function_expr.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
	return data.child_filter_expr.get();
}

static optional_ptr<const PrefixRangeFunctionData> TryGetBitpackingPrefixRangeData(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function_expr = expr.Cast<BoundFunctionExpression>();
	if (function_expr.Function().GetName() != PrefixRangeScalarFun::NAME || !function_expr.BindInfo()) {
		return nullptr;
	}
	return &function_expr.BindInfo()->Cast<PrefixRangeFunctionData>();
}

struct BitpackingPrefixRangeCandidate {
	optional_ptr<const PrefixRangeFunctionData> data;
	bool covers_filter = false;
};

static BitpackingPrefixRangeCandidate TryFindBitpackingPrefixRangeFilter(const Expression &expr) {
	if (auto prefix_data = TryGetBitpackingPrefixRangeData(expr)) {
		return {prefix_data, true};
	}
	if (auto child = TryGetBitpackingSelectivityOptionalChild(expr)) {
		if (!child) {
			return {};
		}
		auto child_candidate = TryFindBitpackingPrefixRangeFilter(*child);
		child_candidate.covers_filter = child_candidate.data && child_candidate.covers_filter;
		return child_candidate;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			auto child_candidate = TryFindBitpackingPrefixRangeFilter(*child);
			if (child_candidate.data) {
				child_candidate.covers_filter = false;
				return child_candidate;
			}
		}
	}
	return {};
}

static BitpackingPrefixRangeCandidate TryGetBitpackingPrefixRangeFilter(const TableFilter &filter,
                                                                        TableFilterState &filter_state) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return {};
	}
	auto &expression_filter = filter.Cast<ExpressionFilter>();
	auto &state = filter_state.Cast<ExpressionFilterState>();
	if (!state.fast_prefix_range_filter_initialized) {
		state.fast_prefix_range_filter_initialized = true;
		auto candidate = TryFindBitpackingPrefixRangeFilter(*expression_filter.expr);
		state.fast_prefix_range_filter_supported = !!candidate.data;
		state.fast_prefix_range_filter_covers_filter = candidate.covers_filter;
		state.fast_prefix_range_filter_data = candidate.data.get();
	}
	if (!state.fast_prefix_range_filter_supported) {
		return {};
	}
	return {optional_ptr<const PrefixRangeFunctionData>(state.fast_prefix_range_filter_data),
	        state.fast_prefix_range_filter_covers_filter};
}

static bool TryGetPrefixLookupSignedInterval(const PrefixRangeLookupData &lookup, int64_t &lower, int64_t &upper) {
	const auto max_signed = static_cast<uint64_t>(NumericLimits<int64_t>::Maximum());
	if (!lookup.bitmap || lookup.min > max_signed || lookup.span > max_signed - lookup.min) {
		return false;
	}
	lower = static_cast<int64_t>(lookup.min);
	upper = static_cast<int64_t>(lookup.min + lookup.span);
	return lower <= upper;
}

static bool SignedRangeContainsPrefixLookup(const SignedNumericRangeFilterData &range,
                                            const PrefixRangeLookupData &lookup) {
	if (range.empty) {
		return false;
	}
	int64_t lookup_lower;
	int64_t lookup_upper;
	if (!TryGetPrefixLookupSignedInterval(lookup, lookup_lower, lookup_upper)) {
		return false;
	}
	return (!range.has_lower || range.lower <= lookup_lower) && (!range.has_upper || range.upper >= lookup_upper);
}

static bool PrefixRangeExpressionCoversFilter(const Expression &expr, const LogicalType &target_type,
                                              const PrefixRangeFunctionData &prefix_data,
                                              const PrefixRangeLookupData &lookup) {
	if (auto expr_prefix_data = TryGetBitpackingPrefixRangeData(expr)) {
		return expr_prefix_data.get() == &prefix_data;
	}
	if (auto child = TryGetBitpackingSelectivityOptionalChild(expr)) {
		return !child || PrefixRangeExpressionCoversFilter(*child, target_type, prefix_data, lookup);
	}

	SignedNumericRangeFilterData range;
	if (TryGetSignedNumericRange(expr, target_type, range)) {
		return SignedRangeContainsPrefixLookup(range, lookup);
	}

	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			if (!PrefixRangeExpressionCoversFilter(*child, target_type, prefix_data, lookup)) {
				return false;
			}
		}
		return true;
	}
	return false;
}

static bool PrefixRangeFilterCoversResidual(const TableFilter &filter, TableFilterState &filter_state,
                                            const LogicalType &target_type, const PrefixRangeFunctionData &prefix_data,
                                            const PrefixRangeLookupData &lookup) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	auto &state = filter_state.Cast<ExpressionFilterState>();
	if (!state.fast_prefix_range_residual_coverage_initialized) {
		state.fast_prefix_range_residual_coverage_initialized = true;
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		state.fast_prefix_range_residual_covers_filter =
		    PrefixRangeExpressionCoversFilter(*expression_filter.expr, target_type, prefix_data, lookup);
	}
	return state.fast_prefix_range_residual_covers_filter;
}

template <class T>
struct BitpackingSignedNumericComparable {
	static constexpr bool SUPPORTED =
	    std::is_integral<T>::value && std::is_signed<T>::value && sizeof(T) <= sizeof(int64_t);

	static int64_t Convert(const T &value) {
		return static_cast<int64_t>(value);
	}
};

template <>
struct BitpackingSignedNumericComparable<hugeint_t> {
	static constexpr bool SUPPORTED = false;

	static int64_t Convert(const hugeint_t &value) {
		return 0;
	}
};

template <>
struct BitpackingSignedNumericComparable<uhugeint_t> {
	static constexpr bool SUPPORTED = false;

	static int64_t Convert(const uhugeint_t &value) {
		return 0;
	}
};

static bool TryGetBitpackingSignedNumericRangeFilter(const TableFilter &filter, TableFilterState &filter_state,
                                                     const LogicalType &target_type,
                                                     SignedNumericRangeFilterData &range) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	auto &expression_filter = filter.Cast<ExpressionFilter>();
	auto &state = filter_state.Cast<ExpressionFilterState>();
	return TryGetSignedNumericRange(expression_filter, state, target_type, range);
}

template <class T>
static inline bool BitpackingSignedNumericRangeMatch(const SignedNumericRangeFilterData &range, const T &value) {
	const auto comparable = BitpackingSignedNumericComparable<T>::Convert(value);
	return (!range.has_lower || comparable >= range.lower) && (!range.has_upper || comparable <= range.upper);
}

template <class T, class T_U = typename MakeUnsigned<T>::type>
static idx_t BitpackingSignedNumericRangeFilterConstantDelta(const BitpackingScanState<T> &scan_state,
                                                             const SignedNumericRangeFilterData &range, T *result_data,
                                                             SelectionVector &result_sel, idx_t result_count,
                                                             const SelectionVector &sel, idx_t selected_idx,
                                                             idx_t selected_end, idx_t scanned) {
	const auto base_offset = scan_state.current_group_offset;
	for (idx_t i = selected_idx; i < selected_end; i++) {
		const auto row_idx = sel.get_index(i);
		auto value = BitpackingConstantDeltaValue<T, T_U>(scan_state, base_offset + row_idx - scanned);
		if (BitpackingSignedNumericRangeMatch(range, value)) {
			result_data[row_idx] = value;
			result_sel.set_index(result_count++, row_idx);
		}
	}
	return result_count;
}

template <class T, class T_S = typename MakeSigned<T>::type>
static bool TryBitpackingSignedNumericRangeFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                                  Vector &result, SelectionVector &sel, idx_t &sel_count,
                                                  const SignedNumericRangeFilterData &range) {
	if (!BitpackingSignedNumericComparable<T>::SUPPORTED ||
	    !IsSignedNumericRangePhysicalType(result.GetType().InternalType())) {
		return false;
	}
	if (!SelectionVectorIsOrdered(sel, sel_count)) {
		return false;
	}

	auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();
	if (range.empty) {
		scan_state.Consume(segment, vector_count);
		sel_count = 0;
		FlatVector::SetSize(result, count_t(vector_count));
		return true;
	}

	auto result_data = FlatVector::GetDataMutable<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &result_sel = BitpackingFilterSelection(scan_state, sel_count);
	idx_t result_count = 0;
	idx_t selected_idx = 0;
	idx_t scanned = 0;
	bool skip_sign_extend = true;
	while (scanned < vector_count) {
		if (selected_idx == sel_count) {
			scan_state.Consume(segment, vector_count - scanned);
			break;
		}
		const auto next_selected = sel.get_index(selected_idx);
		D_ASSERT(next_selected >= scanned);
		if (next_selected > scanned) {
			const auto skip_count = next_selected - scanned;
			scan_state.Skip(segment, skip_count);
			scanned += skip_count;
			continue;
		}

		D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);
		if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			if (BitpackingSignedNumericRangeMatch(range, scan_state.current_constant)) {
				for (idx_t i = selected_idx; i < selected_end; i++) {
					const auto row_idx = sel.get_index(i);
					result_data[row_idx] = scan_state.current_constant;
					result_sel.set_index(result_count++, row_idx);
				}
			}
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			result_count = BitpackingSignedNumericRangeFilterConstantDelta<T>(
			    scan_state, range, result_data, result_sel, result_count, sel, selected_idx, selected_end, scanned);
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}

		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);
		const auto offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		const auto scan_unit_remaining =
		    MinValue<idx_t>(vector_count - scanned,
		                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);
		idx_t selected_end = selected_idx + 1;
		while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
			selected_end++;
		}
		const auto last_selected = sel.get_index(selected_end - 1);
		const auto to_scan = last_selected - scanned + 1;

		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
		                                     decompression_group_start_pointer, scan_state.current_width,
		                                     skip_sign_extend);

		auto decompression_ptr = scan_state.decompression_buffer + offset_in_compression_group;
		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = decompression_ptr[to_scan - 1];
		} else {
			ApplyFrameOfReference<T>(decompression_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		for (idx_t i = selected_idx; i < selected_end; i++) {
			const auto row_idx = sel.get_index(i);
			auto value = decompression_ptr[row_idx - scanned];
			if (BitpackingSignedNumericRangeMatch(range, value)) {
				result_data[row_idx] = value;
				result_sel.set_index(result_count++, row_idx);
			}
		}
		scan_state.current_group_offset += to_scan;
		scanned += to_scan;
		selected_idx = selected_end;
	}
	sel.Initialize(result_sel);
	sel_count = result_count;
	FlatVector::SetSize(result, count_t(vector_count));
	return true;
}

template <class T>
struct BitpackingPrefixComparable {
	static constexpr bool SUPPORTED = std::is_integral<T>::value && sizeof(T) <= sizeof(uint64_t);

	static uint64_t Convert(const T &value) {
		using UNSIGNED_T = typename MakeUnsigned<T>::type;
		return static_cast<uint64_t>(static_cast<UNSIGNED_T>(value));
	}
};

template <>
struct BitpackingPrefixComparable<hugeint_t> {
	static constexpr bool SUPPORTED = false;

	static uint64_t Convert(const hugeint_t &value) {
		return 0;
	}
};

template <>
struct BitpackingPrefixComparable<uhugeint_t> {
	static constexpr bool SUPPORTED = false;

	static uint64_t Convert(const uhugeint_t &value) {
		return 0;
	}
};

#if defined(_MSC_VER)
#define DUCKDB_BITPACKING_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define DUCKDB_BITPACKING_FORCE_INLINE inline __attribute__((always_inline))
#else
#define DUCKDB_BITPACKING_FORCE_INLINE inline
#endif

template <bool EXACT>
static DUCKDB_BITPACKING_FORCE_INLINE bool BitpackingPrefixRangeMatch(uint64_t lookup_min, uint64_t lookup_span,
                                                                      idx_t lookup_shift, const uint64_t *lookup_bitmap,
                                                                      uint64_t comparable) {
	const uint64_t y = comparable - lookup_min;
	if (EXACT) {
		if (y > lookup_span) {
			return false;
		}
		return (lookup_bitmap[y >> 6U] >> (y & 63U)) & 1ULL;
	}
	const uint64_t bit_idx = y >> lookup_shift;
	const uint8_t in_range = y <= lookup_span;
	const uint64_t word_idx = (bit_idx >> 6U) & (0ULL - static_cast<uint64_t>(in_range));
	const uint8_t bit = (lookup_bitmap[word_idx] >> (bit_idx & 63U)) & 1ULL;
	return bit & in_range;
}

template <class T, class MATCHER, class T_U = typename MakeUnsigned<T>::type>
static idx_t BitpackingLookupFilterConstantDelta(const BitpackingScanState<T> &scan_state, MATCHER &matches,
                                                 T *result_data, SelectionVector &result_sel, idx_t result_count,
                                                 const SelectionVector &sel, idx_t selected_idx, idx_t selected_end,
                                                 idx_t scanned) {
	const auto base_offset = scan_state.current_group_offset;
	for (idx_t i = selected_idx; i < selected_end; i++) {
		const auto row_idx = sel.get_index(i);
		auto value = BitpackingConstantDeltaValue<T, T_U>(scan_state, base_offset + row_idx - scanned);
		if (matches(value)) {
			result_data[row_idx] = value;
			result_sel.set_index(result_count++, row_idx);
		}
	}
	return result_count;
}

template <class T, class MATCHER, class T_U = typename MakeUnsigned<T>::type>
static idx_t BitpackingLookupFilterConstantDeltaDense(const BitpackingScanState<T> &scan_state, MATCHER &matches,
                                                      T *result_data, SelectionVector &result_sel, idx_t result_count,
                                                      idx_t scanned, idx_t to_scan) {
	const auto base_offset = scan_state.current_group_offset;
	for (idx_t i = 0; i < to_scan; i++) {
		const auto row_idx = scanned + i;
		auto value = BitpackingConstantDeltaValue<T, T_U>(scan_state, base_offset + i);
		if (matches(value)) {
			result_data[row_idx] = value;
			result_sel.set_index(result_count++, row_idx);
		}
	}
	return result_count;
}

template <class T, class MATCHER, class T_S = typename MakeSigned<T>::type>
static bool TryBitpackingLookupFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                      Vector &result, SelectionVector &sel, idx_t &sel_count, MATCHER &matches) {
	auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();
	auto result_data = FlatVector::GetDataMutable<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &result_sel = BitpackingFilterSelection(scan_state, sel_count);
	idx_t result_count = 0;
	const bool identity_selection =
	    sel_count == vector_count &&
	    (!sel.IsSet() || (sel.get_index(0) == 0 && sel.get_index(sel_count - 1) + 1 == sel_count));
#ifdef DEBUG
	if (identity_selection && sel.IsSet()) {
		for (idx_t i = 0; i < sel_count; i++) {
			D_ASSERT(sel.get_index(i) == i);
		}
	}
#endif
	if (identity_selection) {
		idx_t scanned = 0;
		bool skip_sign_extend = true;
		while (scanned < vector_count) {
			D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);
			if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
				scan_state.LoadNextGroup();
			}

			if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
				const auto to_scan = MinValue<idx_t>(vector_count - scanned,
				                                     BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
				if (matches(scan_state.current_constant)) {
					for (idx_t i = 0; i < to_scan; i++) {
						const auto row_idx = scanned + i;
						result_data[row_idx] = scan_state.current_constant;
						result_sel.set_index(result_count++, row_idx);
					}
				}
				scan_state.current_group_offset += to_scan;
				scanned += to_scan;
				continue;
			}
			if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
				const auto to_scan = MinValue<idx_t>(vector_count - scanned,
				                                     BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
				result_count = BitpackingLookupFilterConstantDeltaDense<T>(scan_state, matches, result_data, result_sel,
				                                                           result_count, scanned, to_scan);
				scan_state.current_group_offset += to_scan;
				scanned += to_scan;
				continue;
			}

			D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
			         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);
			const auto offset_in_compression_group =
			    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
			const auto to_scan =
			    MinValue<idx_t>(vector_count - scanned,
			                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);
			data_ptr_t current_position_ptr =
			    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
			data_ptr_t decompression_group_start_pointer =
			    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

			T *decompression_ptr;
			if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
				decompression_ptr = result_data + scanned;
				BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(decompression_ptr),
				                                     decompression_group_start_pointer, scan_state.current_width,
				                                     skip_sign_extend);
			} else {
				BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
				                                     decompression_group_start_pointer, scan_state.current_width,
				                                     skip_sign_extend);
				decompression_ptr = scan_state.decompression_buffer + offset_in_compression_group;
			}
			if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
				ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
				                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
				DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
				                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
				scan_state.current_delta_offset = decompression_ptr[to_scan - 1];
			} else {
				ApplyFrameOfReference<T>(decompression_ptr, scan_state.current_frame_of_reference, to_scan);
			}

			idx_t i = 0;
			for (; i + 4 <= to_scan; i += 4) {
				const auto row0 = scanned + i;
				const auto row1 = row0 + 1;
				const auto row2 = row0 + 2;
				const auto row3 = row0 + 3;
				const auto value0 = decompression_ptr[i];
				const auto value1 = decompression_ptr[i + 1];
				const auto value2 = decompression_ptr[i + 2];
				const auto value3 = decompression_ptr[i + 3];
				const auto match0 = matches(value0);
				const auto match1 = matches(value1);
				const auto match2 = matches(value2);
				const auto match3 = matches(value3);
				if (match0) {
					result_data[row0] = value0;
					result_sel.set_index(result_count++, row0);
				}
				if (match1) {
					result_data[row1] = value1;
					result_sel.set_index(result_count++, row1);
				}
				if (match2) {
					result_data[row2] = value2;
					result_sel.set_index(result_count++, row2);
				}
				if (match3) {
					result_data[row3] = value3;
					result_sel.set_index(result_count++, row3);
				}
			}
			for (; i < to_scan; i++) {
				const auto row_idx = scanned + i;
				const auto value = decompression_ptr[i];
				if (matches(value)) {
					result_data[row_idx] = value;
					result_sel.set_index(result_count++, row_idx);
				}
			}
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
		}
		sel.Initialize(result_sel);
		sel_count = result_count;
		FlatVector::SetSize(result, count_t(vector_count));
		return true;
	}

	idx_t selected_idx = 0;
	idx_t scanned = 0;
	bool skip_sign_extend = true;
	while (scanned < vector_count) {
		if (selected_idx == sel_count) {
			scan_state.Consume(segment, vector_count - scanned);
			break;
		}
		const auto next_selected = sel.get_index(selected_idx);
		D_ASSERT(next_selected >= scanned);
		if (next_selected > scanned) {
			const auto skip_count = next_selected - scanned;
			scan_state.Skip(segment, skip_count);
			scanned += skip_count;
			continue;
		}

		D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);
		if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			if (matches(scan_state.current_constant)) {
				for (idx_t i = selected_idx; i < selected_end; i++) {
					const auto row_idx = sel.get_index(i);
					result_data[row_idx] = scan_state.current_constant;
					result_sel.set_index(result_count++, row_idx);
				}
			}
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			const auto scan_unit_remaining = MinValue<idx_t>(
			    vector_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			idx_t selected_end = selected_idx + 1;
			while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
				selected_end++;
			}
			const auto last_selected = sel.get_index(selected_end - 1);
			const auto to_scan = last_selected - scanned + 1;
			result_count = BitpackingLookupFilterConstantDelta<T>(
			    scan_state, matches, result_data, result_sel, result_count, sel, selected_idx, selected_end, scanned);
			scan_state.current_group_offset += to_scan;
			scanned += to_scan;
			selected_idx = selected_end;
			continue;
		}

		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);
		const auto offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		const auto scan_unit_remaining =
		    MinValue<idx_t>(vector_count - scanned,
		                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);
		idx_t selected_end = selected_idx + 1;
		while (selected_end < sel_count && sel.get_index(selected_end) < scanned + scan_unit_remaining) {
			selected_end++;
		}
		const auto last_selected = sel.get_index(selected_end - 1);
		const auto to_scan = last_selected - scanned + 1;

		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
		                                     decompression_group_start_pointer, scan_state.current_width,
		                                     skip_sign_extend);

		auto decompression_ptr = scan_state.decompression_buffer + offset_in_compression_group;
		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = decompression_ptr[to_scan - 1];
		} else {
			ApplyFrameOfReference<T>(decompression_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		for (idx_t i = selected_idx; i < selected_end; i++) {
			const auto row_idx = sel.get_index(i);
			auto value = decompression_ptr[row_idx - scanned];
			if (matches(value)) {
				result_data[row_idx] = value;
				result_sel.set_index(result_count++, row_idx);
			}
		}
		scan_state.current_group_offset += to_scan;
		scanned += to_scan;
		selected_idx = selected_end;
	}
	sel.Initialize(result_sel);
	sel_count = result_count;
	FlatVector::SetSize(result, count_t(vector_count));
	return true;
}

template <class T>
static bool TryBitpackingPrefixRangeFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                           Vector &result, SelectionVector &sel, idx_t &sel_count,
                                           const TableFilter &filter, TableFilterState &filter_state,
                                           const PrefixRangeFunctionData &prefix_data, bool &covers_filter) {
	if (!prefix_data.filter || !prefix_data.filter->IsInitialized()) {
		return false;
	}
	if (prefix_data.key_type != result.GetType()) {
		return false;
	}
	if (!BitpackingPrefixComparable<T>::SUPPORTED) {
		return false;
	}
	if (!SelectionVectorIsOrdered(sel, sel_count)) {
		return false;
	}

	PrefixRangeLookupData lookup;
	if (!prefix_data.filter->GetSignedLookupData(lookup) || !lookup.bitmap) {
		return false;
	}
	if (!covers_filter) {
		covers_filter = PrefixRangeFilterCoversResidual(filter, filter_state, result.GetType(), prefix_data, lookup);
	}
	if (lookup.shift == 0) {
		auto matches = [&](const T &value) {
			return BitpackingPrefixRangeMatch<true>(lookup.min, lookup.span, lookup.shift, lookup.bitmap,
			                                        BitpackingPrefixComparable<T>::Convert(value));
		};
		return TryBitpackingLookupFilter<T>(segment, state, vector_count, result, sel, sel_count, matches);
	}
	auto matches = [&](const T &value) {
		return BitpackingPrefixRangeMatch<false>(lookup.min, lookup.span, lookup.shift, lookup.bitmap,
		                                         BitpackingPrefixComparable<T>::Convert(value));
	};
	return TryBitpackingLookupFilter<T>(segment, state, vector_count, result, sel, sel_count, matches);
}

template <class T>
static T BitpackingPerfectHashJoinBound(uint64_t bits) {
	using UNSIGNED_T = typename MakeUnsigned<T>::type;
	const auto unsigned_value = static_cast<UNSIGNED_T>(bits);
	T result;
	memcpy(&result, &unsigned_value, sizeof(T));
	return result;
}

template <class T>
static bool TryBitpackingPerfectHashJoinFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count,
                                               Vector &result, SelectionVector &sel, idx_t &sel_count,
                                               const TableFilter &filter, TableFilterState &filter_state) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER || !std::is_integral<T>::value) {
		return false;
	}
	auto &expression_filter = filter.Cast<ExpressionFilter>();
	auto &filter_state_typed = filter_state.Cast<ExpressionFilterState>();
	if (!ColumnSegment::PrepareInternalFilterPlan(filter_state_typed, *expression_filter.expr, result.GetType())) {
		return false;
	}
	auto &operations = filter_state_typed.fast_internal_filter_operations;
	if (operations.empty() || operations[0].type != FastInternalFilterOperationType::PERFECT_HASH_JOIN ||
	    !operations[0].perfect_hash_join_data || !operations[0].perfect_hash_join_data->executor) {
		return false;
	}

	ExecutionPerfectHashJoinTableLayout layout;
	if (!operations[0].perfect_hash_join_data->executor->GetExecutionPerfectHashJoinTableLayout(layout) ||
	    !layout.ready || layout.key_physical_type != result.GetType().InternalType() ||
	    (!layout.is_build_dense && !layout.build_validity)) {
		return false;
	}
	const auto build_min = BitpackingPerfectHashJoinBound<T>(layout.build_min);
	const auto build_max = BitpackingPerfectHashJoinBound<T>(layout.build_max);
	idx_t fused_operation_count = 1;
	while (fused_operation_count < operations.size() &&
	       operations[fused_operation_count].type == FastInternalFilterOperationType::SIGNED_NUMERIC_RANGE) {
		fused_operation_count++;
	}
	auto matches = [&](const T &value) {
		if (value < build_min || value > build_max) {
			return false;
		}
		const auto build_idx = UnsafeNumericCast<idx_t>(value - build_min);
		if (!layout.is_build_dense && !(layout.build_validity[build_idx / ValidityMask::BITS_PER_VALUE] &
		                                (validity_t(1) << (build_idx % ValidityMask::BITS_PER_VALUE)))) {
			return false;
		}
		for (idx_t operation_idx = 1; operation_idx < fused_operation_count; operation_idx++) {
			auto &operation = operations[operation_idx];
			if (operation.range_empty || (operation.range_has_lower && value < operation.range_lower) ||
			    (operation.range_has_upper && value > operation.range_upper)) {
				return false;
			}
		}
		return true;
	};
	if (!TryBitpackingLookupFilter<T>(segment, state, vector_count, result, sel, sel_count, matches)) {
		return false;
	}
	if (sel_count > 0 && fused_operation_count < operations.size()) {
		UnifiedVectorFormat vdata;
		result.ToUnifiedFormat(vdata);
		ColumnSegment::ApplyInternalFilterPlan(filter_state_typed, sel, result, vdata, sel_count,
		                                       fused_operation_count);
	}
	return true;
}

template <class T>
void BitpackingFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                      SelectionVector &sel, idx_t &sel_count, const TableFilter &filter,
                      TableFilterState &filter_state) {
	if (sel_count == 0) {
		auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();
		scan_state.Consume(segment, vector_count);
		return;
	}
	if (TryBitpackingPerfectHashJoinFilter<T>(segment, state, vector_count, result, sel, sel_count, filter,
	                                          filter_state)) {
		return;
	}
	SignedNumericRangeFilterData signed_range;
	if (TryGetBitpackingSignedNumericRangeFilter(filter, filter_state, result.GetType(), signed_range) &&
	    TryBitpackingSignedNumericRangeFilter<T>(segment, state, vector_count, result, sel, sel_count, signed_range)) {
		return;
	}
	auto prefix_candidate = TryGetBitpackingPrefixRangeFilter(filter, filter_state);
	if (prefix_candidate.data) {
		if (TryBitpackingPrefixRangeFilter<T>(segment, state, vector_count, result, sel, sel_count, filter,
		                                      filter_state, *prefix_candidate.data, prefix_candidate.covers_filter)) {
			if (prefix_candidate.covers_filter) {
				return;
			}
			UnifiedVectorFormat vdata;
			result.ToUnifiedFormat(vdata);
			ColumnSegment::FilterSelection(sel, result, vdata, filter, filter_state, vector_count, sel_count);
			return;
		}
	}
	if (ShouldUseBitpackingSelectedFilter(vector_count, sel_count) && SelectionVectorIsOrdered(sel, sel_count)) {
		BitpackingScanSelected<T>(segment, state, vector_count, result, sel, sel_count);
	} else {
		BitpackingScanPartial<T>(segment, state, vector_count, result, 0);
	}
	FlatVector::SetSize(result, count_t(vector_count));

	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(vdata);
	ColumnSegment::FilterSelection(sel, result, vdata, filter, filter_state, vector_count, sel_count);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                        idx_t result_idx) {
	BitpackingScanState<T> scan_state(state.context, segment);
	scan_state.Skip(segment, NumericCast<idx_t>(row_id));

	D_ASSERT(scan_state.current_group_offset < BITPACKING_METADATA_GROUP_SIZE);

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	T *result_data = FlatVector::GetDataMutable<T>(result);
	T *current_result_ptr = result_data + result_idx;

	idx_t offset_in_compression_group =
	    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	data_ptr_t decompression_group_start_pointer =
	    scan_state.current_group_ptr +
	    (scan_state.current_group_offset - offset_in_compression_group) * scan_state.current_width / 8;

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
		*current_result_ptr = scan_state.current_constant;
		return;
	}

	if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
		T multiplier;
		auto cast = TryCast::Operation<idx_t, T>(scan_state.current_group_offset, multiplier);
		(void)cast;
		D_ASSERT(cast);
#ifdef DEBUG
		// overflow check
		T result;
		bool multiply = TryMultiplyOperator::Operation(multiplier, scan_state.current_constant, result);
		bool add = TryAddOperator::Operation(result, scan_state.current_frame_of_reference, result);
		D_ASSERT(multiply && add);
#endif
		*current_result_ptr = (multiplier * scan_state.current_constant) + scan_state.current_frame_of_reference;
		return;
	}

	D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
	         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

	BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
	                                     decompression_group_start_pointer, scan_state.current_width, skip_sign_extend);

	*current_result_ptr = scan_state.decompression_buffer[offset_in_compression_group];
	*current_result_ptr += scan_state.current_frame_of_reference;

	if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
		*current_result_ptr += scan_state.current_delta_offset;
	}
}

template <class T>
void BitpackingSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = static_cast<BitpackingScanState<T> &>(*state.scan_state);
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// GetSegmentInfo
//===--------------------------------------------------------------------===//
template <class T>
InsertionOrderPreservingMap<string> BitpackingGetSegmentInfo(QueryContext context, ColumnSegment &segment) {
	map<BitpackingMode, idx_t> counts;
	auto tuple_count = segment.count.load();
	BitpackingScanState<T> scan_state(context, segment);
	for (idx_t i = 0; i < tuple_count; i += BITPACKING_METADATA_GROUP_SIZE) {
		if (i) {
			scan_state.LoadNextGroup();
		}
		counts[scan_state.current_group.mode]++;
	}

	InsertionOrderPreservingMap<string> result;
	for (auto &it : counts) {
		auto &mode = it.first;
		auto &count = it.second;
		result[EnumUtil::ToString(mode)] = StringUtil::Format("%d", count);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS = true>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	auto bitpacking = CompressionFunction(
	    CompressionType::COMPRESSION_BITPACKING, data_type, BitpackingInitAnalyze<T>, BitpackingAnalyze<T>,
	    BitpackingFinalAnalyze<T>, BitpackingInitCompression<T, WRITE_STATISTICS>,
	    BitpackingCompress<T, WRITE_STATISTICS>, BitpackingFinalizeCompress<T, WRITE_STATISTICS>, BitpackingInitScan<T>,
	    BitpackingScan<T>, BitpackingScanPartial<T>, BitpackingFetchRow<T>, BitpackingSkip<T>);
	bitpacking.filter = BitpackingFilter<T>;
	bitpacking.select = BitpackingSelect<T>;
	bitpacking.get_segment_info = BitpackingGetSegmentInfo<T>;
	return bitpacking;
}

CompressionFunction BitpackingFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetBitpackingFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetBitpackingFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
	case PhysicalType::UINT8:
		return GetBitpackingFunction<uint8_t>(type);
	case PhysicalType::UINT16:
		return GetBitpackingFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetBitpackingFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetBitpackingFunction<uint64_t>(type);
	case PhysicalType::INT128:
		return GetBitpackingFunction<hugeint_t>(type);
	case PhysicalType::UINT128:
		return GetBitpackingFunction<uhugeint_t>(type);
	case PhysicalType::LIST:
		return GetBitpackingFunction<uint64_t, false>(type);
	default:
		throw InternalException("Unsupported type for Bitpacking");
	}
}

bool BitpackingFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::LIST:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb

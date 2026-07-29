//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_matched_row_batch_consumer_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"

#include <array>

namespace duckdb {

// A bounded match microbatch keeps probe and consumer ownership fused without
// instantiating every probe-key type against every terminal-consumer type. The
// batch is small enough to remain in L1 and invokes the typed terminal only once
// per group of matches rather than through an indirect call for every row.
struct SljitHashJoinMatchedRowBatchConsumer {
	static constexpr idx_t CAPACITY = 32;

	template <class CONSUMER>
	SljitHashJoinMatchedRowBatchConsumer(SljitNativeRegularHashJoinProbeInput &input_p, CONSUMER &consumer)
	    : input(input_p), context(&consumer), consume_batch(ConsumeBatch<CONSUMER>) {
	}

	inline void EmitNoChainMatch(idx_t row_idx, data_ptr_t row_location) {
		row_indices[buffered_count] = UnsafeNumericCast<sel_t>(row_idx);
		row_pointers[buffered_count] = row_location;
		buffered_count++;
		matched_count++;
		if (buffered_count == CAPACITY) {
			Flush();
		}
	}

	void Finish() {
		Flush();
		input.selected_count = matched_count;
		input.input_offset = input.count;
		input.resume_row_pointer = nullptr;
		input.finished = true;
	}

	idx_t MatchedCount() const {
		return matched_count;
	}

private:
	template <class CONSUMER>
	static void ConsumeBatch(void *context, const sel_t *row_indices, data_ptr_t const *row_pointers, idx_t count) {
		static_cast<CONSUMER *>(context)->Consume(row_indices, row_pointers, count);
	}

	inline void Flush() {
		if (buffered_count == 0) {
			return;
		}
		consume_batch(context, row_indices.data(), row_pointers.data(), buffered_count);
		buffered_count = 0;
	}

	using ConsumeBatchFunction = void (*)(void *, const sel_t *, data_ptr_t const *, idx_t);

	SljitNativeRegularHashJoinProbeInput &input;
	void *context;
	ConsumeBatchFunction consume_batch;
	std::array<sel_t, CAPACITY> row_indices {};
	std::array<data_ptr_t, CAPACITY> row_pointers {};
	idx_t buffered_count = 0;
	idx_t matched_count = 0;
};

} // namespace duckdb

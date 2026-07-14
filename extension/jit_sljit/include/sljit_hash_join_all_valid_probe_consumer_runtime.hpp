//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_consumer_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_base_runtime.hpp"

#include <array>

namespace duckdb {

struct SljitHashJoinChainMatchResult {
	bool output_full;
	bool row_finished;
};

template <bool MARK_BUILD_MATCH>
struct SljitHashJoinBuildMatchMarker {
	explicit SljitHashJoinBuildMatchMarker(const SljitNativeHashJoinProbePlan &plan)
	    : found_match_offset(plan.found_match_offset) {
	}

	inline void Mark(data_ptr_t row_location) const {
		row_location[found_match_offset] = 1;
	}

	idx_t found_match_offset;
};

template <>
struct SljitHashJoinBuildMatchMarker<false> {
	explicit SljitHashJoinBuildMatchMarker(const SljitNativeHashJoinProbePlan &) {
	}

	inline void Mark(data_ptr_t) const {
	}
};

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

template <bool MARK_BUILD_MATCH, bool MATCHED_PROBE_ONLY>
struct SljitHashJoinMatchedRowConsumer {
	SljitHashJoinMatchedRowConsumer(SljitNativeRegularHashJoinProbeInput &input,
	                                const SljitNativeHashJoinProbePlan &plan)
	    : input(input), row_pointers(input.row_pointers), match_sel(input.match_sel),
	      selected_count(input.selected_count), output_capacity(input.output_capacity), build_match_marker(plan) {
	}

	inline void EmitNoChainMatch(const idx_t row_idx, const data_ptr_t row_location) {
		Emit(row_idx, row_location);
	}

	inline SljitHashJoinChainMatchResult EmitChainMatch(idx_t &row_idx, const data_ptr_t row_location,
	                                                    const data_ptr_t next_row_location) {
		Emit(row_idx, row_location);
		if constexpr (MATCHED_PROBE_ONLY) {
			row_idx++;
			if (selected_count >= output_capacity) {
				Finish(row_idx, nullptr);
				return {true, true};
			}
			return {false, true};
		}
		if (selected_count < output_capacity) {
			return {false, false};
		}
		Finish(next_row_location ? row_idx : row_idx + 1, next_row_location);
		return {true, false};
	}

	void Finish() {
		Finish(input.count, nullptr);
	}

private:
	inline void Emit(const idx_t row_idx, const data_ptr_t row_location) {
		build_match_marker.Mark(row_location);
		row_pointers[selected_count] = row_location;
		match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
		selected_count++;
	}

	void Finish(const idx_t next_row_idx, const data_ptr_t next_row_location) {
		input.selected_count = selected_count;
		input.input_offset = next_row_idx;
		input.resume_row_pointer = next_row_location;
		input.finished = !next_row_location && input.input_offset >= input.count;
	}

	SljitNativeRegularHashJoinProbeInput &input;
	data_ptr_t *__restrict row_pointers;
	sel_t *__restrict match_sel;
	idx_t selected_count;
	idx_t output_capacity;
	SljitHashJoinBuildMatchMarker<MARK_BUILD_MATCH> build_match_marker;
};

template <SljitHashJoinMarkSelectionMode MODE, bool MARK_BUILD_MATCH>
struct SljitHashJoinMarkSelectionConsumer {
	SljitHashJoinMarkSelectionConsumer(SljitNativeRegularHashJoinProbeInput &input,
	                                   const SljitNativeHashJoinProbePlan &plan)
	    : input(input), match_sel(input.match_sel), selected_count(input.selected_count), build_match_marker(plan) {
	}

	inline void EmitMatch(const idx_t row_idx, const data_ptr_t row_location) {
		build_match_marker.Mark(row_location);
		if constexpr (MODE == SljitHashJoinMarkSelectionMode::MATCHES) {
			Emit(row_idx);
		}
	}

	inline void EmitNoMatch(const idx_t row_idx) {
		if constexpr (MODE == SljitHashJoinMarkSelectionMode::NON_MATCHES) {
			Emit(row_idx);
		}
	}

	void Finish() {
		input.selected_count = selected_count;
		input.input_offset = input.count;
		input.resume_row_pointer = nullptr;
		input.finished = true;
	}

private:
	inline void Emit(const idx_t row_idx) {
		match_sel[selected_count++] = UnsafeNumericCast<sel_t>(row_idx);
	}

	SljitNativeRegularHashJoinProbeInput &input;
	sel_t *__restrict match_sel;
	idx_t selected_count;
	SljitHashJoinBuildMatchMarker<MARK_BUILD_MATCH> build_match_marker;
};

} // namespace duckdb

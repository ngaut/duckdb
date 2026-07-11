//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_exact_perfect_hash_join_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

template <class TARGET>
static TARGET SljitExactPerfectHashJoinMinimum(uint64_t bits) {
	TARGET result;
	static_assert(sizeof(TARGET) <= sizeof(bits), "perfect hash join target must fit in the runtime layout word");
	memcpy(&result, &bits, sizeof(TARGET));
	return result;
}

template <class TARGET, class SOURCE = TARGET>
static void SljitPopulateExactPerfectHashJoinSelections(SljitNativePerfectHashJoinProbeInput &input) {
	const auto source = reinterpret_cast<const SOURCE *>(input.source_data);
	const auto min_value = SljitExactPerfectHashJoinMinimum<TARGET>(input.perfect_min);
	const auto max_value = SljitExactPerfectHashJoinMinimum<TARGET>(input.perfect_max);
	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < input.count; row_idx++) {
		const auto source_idx = input.source_sel ? input.source_sel[row_idx] : row_idx;
		TARGET value;
		if constexpr (std::is_same<TARGET, SOURCE>::value) {
			// Identity equality already proves exact membership for the native key type.
			value = source[source_idx];
		} else {
			// A wider scan key still crosses a checked proof boundary. Validate both conversion and
			// the perfect-table domain before deriving its build offset.
			if (!TryCast::Operation(source[source_idx], value) || value < min_value || value > max_value) {
				continue;
			}
		}
		input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
		input.build_sel[selected_count] = UnsafeNumericCast<sel_t>(value - min_value);
		selected_count++;
	}
	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.finished = true;
}

} // namespace duckdb

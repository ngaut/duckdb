//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_register_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_platform.hpp"

namespace duckdb {

struct SljitNativeVectorRegisterLayout {
	sljit_s32 optional_invariant = 0;
	sljit_s32 saved_register_count = 0;

	bool Available() const {
		return saved_register_count >= 6;
	}

	bool HasOptionalInvariant() const {
		return optional_invariant != 0;
	}
};

struct SljitPerfectHashRegisterLayout {
	bool has_dedicated_state = false;
	bool has_group_data = false;
	sljit_s32 state = 0;
	sljit_s32 reduction_state = 0;
	sljit_s32 group_data[2] = {0, 0};
	sljit_s32 saved_register_count = 0;
	sljit_s32 group_data_saved_register_count = 0;
};

struct SljitUngroupedAggregateRegisterLayout {
	bool has_source_data_hoists = false;
	//! Running sums live in scratch registers past the five every emitter uses: the fast loops that keep them call no
	//! helper except on the way out with an error
	bool has_register_accumulators = false;
	sljit_s32 source_data_saved_register_count = 0;
	sljit_s32 accumulator_scratch_register_count = 0;
	sljit_s32 source_data[2] = {0, 0};
};

struct SljitPerfectHashProbeRegisterLayout {
	bool has_invariant_hoists = false;
	sljit_s32 scratch_register_count = 5;
	sljit_s32 saved_register_count = 0;
	//! With invariant hoists: the output selections stay in scratch registers: a probe loop calls a helper only to
	//! raise an error
	sljit_s32 match_selection = 0;
	sljit_s32 build_selection = 0;
	sljit_s32 source_data = 0;
	sljit_s32 source_selection = 0;
	sljit_s32 source_validity = 0;
	sljit_s32 minimum = 0;
	sljit_s32 maximum = 0;
	sljit_s32 perfect_validity = 0;
	sljit_s32 minimum_lower = 0;
	sljit_s32 minimum_upper = 0;
	sljit_s32 maximum_lower = 0;
};

const SljitNativeVectorRegisterLayout &GetSljitNativeVectorRegisterLayout();
const SljitPerfectHashRegisterLayout &GetSljitPerfectHashRegisterLayout();
const SljitUngroupedAggregateRegisterLayout &GetSljitUngroupedAggregateRegisterLayout();
const SljitPerfectHashProbeRegisterLayout &GetSljitPerfectHashProbeRegisterLayout();

} // namespace duckdb

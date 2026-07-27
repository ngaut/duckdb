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
	sljit_s32 input = 0;
	sljit_s32 row_index = 0;
	sljit_s32 row_count = 0;
	sljit_s32 logical_index = 0;
	sljit_s32 source_selection = 0;
	sljit_s32 source_data = 0;
	sljit_s32 optional_invariant = 0;
	sljit_s32 saved_register_count = 0;

	bool Available() const {
		return input != 0;
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
	bool has_conditional_hugeint_accumulators = false;
	sljit_s32 source_data_saved_register_count = 0;
	sljit_s32 conditional_hugeint_saved_register_count = 0;
	sljit_s32 source_data[2] = {0, 0};
	sljit_s32 fast_source_data = 0;
	sljit_s32 shared_lower = 0;
	sljit_s32 shared_upper = 0;
	sljit_s32 conditional_lower = 0;
	sljit_s32 conditional_upper = 0;
	sljit_s32 conditional_payload = 0;
};

struct SljitPrimitiveRunRegisterLayout {
	bool supported = false;
	bool has_output_pointer_hoists = false;
	bool has_affine_accumulators = false;
	sljit_s32 output_group_data = 0;
	sljit_s32 output_row_counts = 0;
	sljit_s32 output_values = 0;
	sljit_s32 output_value_is_set = 0;
	sljit_s32 affine_value = 0;
	sljit_s32 affine_valid_count = 0;
	sljit_s32 base_saved_register_count = 0;
	sljit_s32 saved_register_count = 0;
	sljit_s32 affine_saved_register_count = 0;
};

struct SljitPerfectHashProbeRegisterLayout {
	bool has_invariant_hoists = false;
	sljit_s32 saved_register_count = 0;
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
const SljitPrimitiveRunRegisterLayout &GetSljitPrimitiveRunRegisterLayout();
const SljitPerfectHashProbeRegisterLayout &GetSljitPerfectHashProbeRegisterLayout();

} // namespace duckdb

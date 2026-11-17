#include "sljit_register_layout.hpp"

namespace duckdb {

static SljitNativeVectorRegisterLayout BuildSljitNativeVectorRegisterLayout() {
	SljitNativeVectorRegisterLayout result;
	SljitSavedRegisterAllocator registers(5, 6);
	if (!registers.Valid()) {
		return result;
	}
	result.optional_invariant = registers.Allocate();
	result.saved_register_count = registers.SavedRegisterCount();
	return result;
}

static SljitPredicateDictionaryResidualRegisterLayout BuildSljitPredicateDictionaryResidualRegisterLayout() {
	SljitPredicateDictionaryResidualRegisterLayout result;
	const auto &native_vector = GetSljitNativeVectorRegisterLayout();
	const auto saved_register_count = native_vector.saved_register_count + 3;
	if (!native_vector.HasOptionalInvariant() ||
	    !GetSljitTargetCapabilities().registers.SupportsLayout(5, saved_register_count)) {
		return result;
	}
	result.pass_table = SljitSavedRegisterAt(native_vector.saved_register_count);
	result.dictionary_selection = SljitSavedRegisterAt(native_vector.saved_register_count + 1);
	result.match_count = SljitSavedRegisterAt(native_vector.saved_register_count + 2);
	result.saved_register_count = saved_register_count;
	return result;
}

static SljitPerfectHashRegisterLayout BuildSljitPerfectHashRegisterLayout() {
	SljitPerfectHashRegisterLayout result;
	const auto &registers = GetSljitTargetCapabilities().registers;
	const auto &native_vector = GetSljitNativeVectorRegisterLayout();
	if (!native_vector.Available()) {
		return result;
	}
	result.state = SljitSavedRegisterAt(4);
	result.reduction_state = result.state;
	result.saved_register_count = native_vector.saved_register_count;
	result.group_data_saved_register_count = result.saved_register_count;

	if (registers.SupportsLayout(5, 8)) {
		result.has_dedicated_state = true;
		result.state = SljitSavedRegisterAt(7);
		result.reduction_state = result.state;
		result.saved_register_count = 8;
		result.group_data_saved_register_count = result.saved_register_count;
	}
	if (registers.SupportsLayout(5, 10)) {
		result.has_group_data = true;
		result.group_data[0] = SljitSavedRegisterAt(8);
		result.group_data[1] = SljitSavedRegisterAt(9);
		result.group_data_saved_register_count = 10;
	}
	return result;
}

static SljitUngroupedAggregateRegisterLayout BuildSljitUngroupedAggregateRegisterLayout() {
	SljitUngroupedAggregateRegisterLayout result;
	const auto &capabilities = GetSljitTargetCapabilities();
	result.source_data_saved_register_count = GetSljitNativeVectorRegisterLayout().saved_register_count;
	if (capabilities.registers.SupportsLayout(5, 10)) {
		result.has_source_data_hoists = true;
		result.source_data_saved_register_count = 10;
		result.source_data[0] = SljitSavedRegisterAt(8);
		result.source_data[1] = SljitSavedRegisterAt(9);
		result.fast_source_data = SljitSavedRegisterAt(6);
	}
	constexpr sljit_s32 accumulator_scratch_register_count = 10;
	if (capabilities.registers.CanEnter(accumulator_scratch_register_count, result.source_data_saved_register_count)) {
		result.has_register_accumulators = true;
		result.accumulator_scratch_register_count = accumulator_scratch_register_count;
		result.shared_lower = SLJIT_R5;
		result.shared_upper = SLJIT_R6;
		result.conditional_lower = SLJIT_R7;
		result.conditional_upper = SLJIT_R8;
		result.conditional_payload = SLJIT_R9;
	}
	return result;
}

static SljitPrimitiveRunRegisterLayout BuildSljitPrimitiveRunRegisterLayout() {
	SljitPrimitiveRunRegisterLayout result;
	const auto &capabilities = GetSljitTargetCapabilities();
	result.supported = capabilities.SupportsPrimitiveRunRegisterABI();
	if (!result.supported) {
		return result;
	}
	result.base_saved_register_count = 6;
	result.saved_register_count = result.base_saved_register_count;
	result.affine_saved_register_count = result.base_saved_register_count;
	result.multi_saved_register_count = result.base_saved_register_count;
	result.shared_validity_multi_saved_register_count = result.base_saved_register_count;
	if (capabilities.registers.SupportsLayout(7, 7)) {
		result.has_multi_output_index = true;
		result.multi_output_index = SljitSavedRegisterAt(6);
		result.multi_saved_register_count = 7;
		result.shared_validity_multi_saved_register_count = 7;
	}
	if (capabilities.registers.SupportsLayout(7, 8)) {
		result.has_multi_output_row_counts = true;
		result.multi_output_row_counts = SljitSavedRegisterAt(7);
		result.multi_saved_register_count = 8;
		result.shared_validity_multi_saved_register_count = 8;
	}
	if (capabilities.registers.SupportsLayout(7, 9)) {
		result.has_multi_shared_validity = true;
		result.multi_shared_validity = SljitSavedRegisterAt(8);
		result.shared_validity_multi_saved_register_count = 9;
	}
	if (capabilities.registers.SupportsLayout(7, 8)) {
		result.has_affine_accumulators = true;
		result.affine_value = SljitSavedRegisterAt(6);
		result.affine_valid_count = SljitSavedRegisterAt(7);
		result.affine_saved_register_count = 8;
	}
	if (capabilities.registers.SupportsLayout(7, 10)) {
		result.has_output_pointer_hoists = true;
		result.output_group_data = SljitSavedRegisterAt(6);
		result.output_row_counts = SljitSavedRegisterAt(7);
		result.output_values = SljitSavedRegisterAt(8);
		result.output_value_is_set = SljitSavedRegisterAt(9);
		result.saved_register_count = 10;
	}
	return result;
}

static SljitPerfectHashProbeRegisterLayout BuildSljitPerfectHashProbeRegisterLayout() {
	SljitPerfectHashProbeRegisterLayout result;
	const auto &registers = GetSljitTargetCapabilities().registers;
	if (!registers.SupportsLayout(5, 5)) {
		return result;
	}
	result.saved_register_count = 5;
	result.source_data = SljitSavedRegisterAt(4);
	if (registers.SupportsLayout(5, 10)) {
		result.has_invariant_hoists = true;
		result.saved_register_count = 10;
		result.source_selection = SljitSavedRegisterAt(5);
		result.source_validity = SljitSavedRegisterAt(6);
		result.minimum = SljitSavedRegisterAt(7);
		result.maximum = SljitSavedRegisterAt(8);
		result.perfect_validity = SljitSavedRegisterAt(9);
		result.minimum_lower = SljitSavedRegisterAt(7);
		result.minimum_upper = SljitSavedRegisterAt(8);
		result.maximum_lower = SljitSavedRegisterAt(9);
	}
	return result;
}

const SljitNativeVectorRegisterLayout &GetSljitNativeVectorRegisterLayout() {
	static const auto result = BuildSljitNativeVectorRegisterLayout();
	return result;
}

const SljitPredicateDictionaryResidualRegisterLayout &GetSljitPredicateDictionaryResidualRegisterLayout() {
	static const auto result = BuildSljitPredicateDictionaryResidualRegisterLayout();
	return result;
}

const SljitPerfectHashRegisterLayout &GetSljitPerfectHashRegisterLayout() {
	static const auto result = BuildSljitPerfectHashRegisterLayout();
	return result;
}

const SljitUngroupedAggregateRegisterLayout &GetSljitUngroupedAggregateRegisterLayout() {
	static const auto result = BuildSljitUngroupedAggregateRegisterLayout();
	return result;
}

const SljitPrimitiveRunRegisterLayout &GetSljitPrimitiveRunRegisterLayout() {
	static const auto result = BuildSljitPrimitiveRunRegisterLayout();
	return result;
}

const SljitPerfectHashProbeRegisterLayout &GetSljitPerfectHashProbeRegisterLayout() {
	static const auto result = BuildSljitPerfectHashProbeRegisterLayout();
	return result;
}

} // namespace duckdb

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

const SljitNativeVectorRegisterLayout &GetSljitNativeVectorRegisterLayout() {
	static const auto result = BuildSljitNativeVectorRegisterLayout();
	return result;
}

const SljitPerfectHashRegisterLayout &GetSljitPerfectHashRegisterLayout() {
	static const auto result = BuildSljitPerfectHashRegisterLayout();
	return result;
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_codegen_validation.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_hash_join_probe_codegen_validation.hpp"

#include "sljit_region_executable.hpp"

namespace duckdb {

static bool SljitValidateHashJoinProbeBase(const SljitNativeHashJoinProbePlan &plan, string &error) {
	if (plan.keys.empty()) {
		error = "SLJIT hash join probe requires at least one key";
		return false;
	}
	if (plan.equality_key_count == 0 || plan.equality_key_count > plan.keys.size()) {
		error = "SLJIT hash join probe requires an equality-key prefix";
		return false;
	}
	for (idx_t key_idx = 0; key_idx < plan.keys.size(); key_idx++) {
		if ((key_idx < plan.equality_key_count) != plan.keys[key_idx].equality_key) {
			error = "SLJIT hash join probe key plan is not an equality-key prefix";
			return false;
		}
	}
	if (plan.output_mode == ExecutionHashJoinProbeOutputMode::NONE) {
		error = "SLJIT hash join probe requires an output mode";
		return false;
	}
	return true;
}

bool SljitValidatePerfectHashJoinProbePlan(const SljitNativeHashJoinProbePlan &plan, string &error) {
	if (!SljitValidateHashJoinProbeBase(plan, error)) {
		return false;
	}
	if (!plan.perfect_hash_probe) {
		error = "SLJIT perfect hash join probe requires a perfect hash plan";
		return false;
	}
	if (plan.keys.size() != 1) {
		error = "SLJIT perfect hash join probe requires exactly one key";
		return false;
	}
	auto &key = plan.keys[0];
	if (!key.equality_key) {
		error = "SLJIT perfect hash join probe requires an equality key";
		return false;
	}
	if (key.key_kind == SljitNativeHashJoinKeyKind::INT128 || key.key_kind == SljitNativeHashJoinKeyKind::UINT128) {
		error = "SLJIT perfect hash join probe does not support 128-bit keys";
		return false;
	}
	if (plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		error = "SLJIT perfect hash join probe requires an inner output mode";
		return false;
	}
	return true;
}

bool SljitValidateRegularHashJoinProbePlan(const SljitNativeHashJoinProbePlan &plan, string &error) {
	if (!SljitValidateHashJoinProbeBase(plan, error)) {
		return false;
	}
	if (plan.perfect_hash_probe) {
		error = "SLJIT regular hash join probe received a perfect hash plan";
		return false;
	}
	return true;
}

bool SljitValidateHashJoinProbePlan(const SljitNativeHashJoinProbePlan &plan, string &error) {
	return plan.perfect_hash_probe ? SljitValidatePerfectHashJoinProbePlan(plan, error)
	                               : SljitValidateRegularHashJoinProbePlan(plan, error);
}

bool SljitExecutableHashJoinProbe::ValidateDeferredCodegen(string &error) const {
	return SljitValidateHashJoinProbePlan(plan, error);
}

bool SljitExecutableHashJoinProbe::HasDeferredCodegen() const {
	string unused_error;
	return ValidateDeferredCodegen(unused_error);
}

} // namespace duckdb

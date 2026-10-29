//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_execution_contract.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_hash_join_runtime.hpp"

namespace duckdb {

enum class SljitHashJoinProbeOutputContract {
	MATERIALIZED_OUTPUT,
	SELECTED_VIEW,
	//! A direct consumer can derive a perfect-hash build index from its proven
	//! identity input. The generated probe may elide both temporary selections,
	//! but retries the compact-selection kernel on the first miss.
	IDENTITY_PREFERRED_DIRECT_CONSUMER,
	FILTERED_MARK_MATCHES,
	FILTERED_MARK_NON_MATCHES
};

static bool SljitHashJoinProbeOutputIsFilteredMarkMatches(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::FILTERED_MARK_MATCHES;
}

static bool SljitHashJoinProbeOutputIsFilteredMarkNonMatches(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::FILTERED_MARK_NON_MATCHES;
}

static SljitHashJoinMarkSelectionMode
SljitHashJoinMarkSelectionModeForOutputContract(SljitHashJoinProbeOutputContract output_contract) {
	if (SljitHashJoinProbeOutputIsFilteredMarkMatches(output_contract)) {
		return SljitHashJoinMarkSelectionMode::MATCHES;
	}
	if (SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract)) {
		return SljitHashJoinMarkSelectionMode::NON_MATCHES;
	}
	return SljitHashJoinMarkSelectionMode::NONE;
}

static bool SljitHashJoinProbeProducesSelectedView(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::SELECTED_VIEW ||
	       output_contract == SljitHashJoinProbeOutputContract::IDENTITY_PREFERRED_DIRECT_CONSUMER ||
	       SljitHashJoinProbeOutputIsFilteredMarkMatches(output_contract) ||
	       SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract);
}

static bool SljitHashJoinProbePrefersIdentitySelection(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::IDENTITY_PREFERRED_DIRECT_CONSUMER;
}

static bool SljitHashJoinProbePrefersDirectConsumerOutput(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::IDENTITY_PREFERRED_DIRECT_CONSUMER;
}

static const SljitNativeHashJoinProbePlan &
SljitBindHashJoinProbeExecutionPlan(const SljitExecutableRegionOp &op,
                                    optional_ptr<const SljitNativeHashJoinProbePlan> input_plan,
                                    SljitHashJoinProbeOutputContract output_contract) {
	if (!input_plan) {
		return op.HashJoinProbe().plan;
	}
	if (!SljitHashJoinProbeProducesSelectedView(output_contract)) {
		throw InternalException("SLJIT prepared hash join probe plan requires selected-view execution");
	}
	return *input_plan;
}

} // namespace duckdb

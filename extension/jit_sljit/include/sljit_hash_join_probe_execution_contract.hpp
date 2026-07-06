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
	       SljitHashJoinProbeOutputIsFilteredMarkMatches(output_contract) ||
	       SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract);
}

struct SljitHashJoinProbeExecutionContractView {
	const SljitNativeHashJoinProbePlan *plan = nullptr;
	const ExecutionRegionOperatorInfo *operator_info = nullptr;
};

static SljitHashJoinProbeExecutionContractView
SljitBuildHashJoinProbeExecutionContractView(SljitExecutableRegionOp &op,
                                             optional_ptr<const SljitHashJoinProbeInputRemap> input_remap,
                                             SljitHashJoinProbeOutputContract output_contract) {
	auto &plan = op.hash_join_probe.plan;
	SljitHashJoinProbeExecutionContractView view;
	view.plan = &plan;
	view.operator_info = &plan.operator_info;
	if (!input_remap || !input_remap->HasRemap()) {
		return view;
	}
	if (!SljitHashJoinProbeProducesSelectedView(output_contract)) {
		throw InternalException("SLJIT prepared hash join probe remap requires selected-view execution");
	}
	if (!input_remap->has_prepared_plan) {
		throw InternalException("SLJIT hash join probe remap was not prepared during primitive binding");
	}
	view.plan = &input_remap->prepared_plan;
	view.operator_info = &input_remap->prepared_plan.operator_info;
	return view;
}

} // namespace duckdb

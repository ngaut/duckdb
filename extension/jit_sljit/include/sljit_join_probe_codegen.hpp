//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_probe_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"
#include "sljit_hash_join_probe_specialization.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_region_plan.hpp"

#include "duckdb/execution/execution_region_telemetry.hpp"

namespace duckdb {

struct SljitHashJoinProbeCodegenConfig {
	SljitHashJoinProbeInputKind input_kind = SljitHashJoinProbeInputKind::GENERIC;
	SljitHashJoinProbeLayoutKind layout_kind = SljitHashJoinProbeLayoutKind::RUNTIME;
	bool uses_bloom_filter = false;
	SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE;

	static SljitHashJoinProbeCodegenConfig
	ForAllValidSpecialization(const SljitHashJoinProbeAllValidSpecializationKey &key) {
		SljitHashJoinProbeCodegenConfig config;
		config.input_kind = key.selected ? SljitHashJoinProbeInputKind::SELECTED_ALL_VALID
		                                 : SljitHashJoinProbeInputKind::FLAT_ALL_VALID;
		config.layout_kind = key.layout_kind;
		return config;
	}

	bool AssumesFlatAllValid() const {
		return input_kind == SljitHashJoinProbeInputKind::FLAT_ALL_VALID;
	}

	bool AssumesCommonSelectionAllValid() const {
		return input_kind == SljitHashJoinProbeInputKind::SELECTED_ALL_VALID;
	}

	bool AssumesAllKeysValid() const {
		return input_kind != SljitHashJoinProbeInputKind::GENERIC;
	}

	bool HasRuntimeLayout() const {
		return layout_kind == SljitHashJoinProbeLayoutKind::RUNTIME;
	}

	bool UsesSalt() const {
		return SljitHashJoinProbeLayoutUsesSalt(layout_kind);
	}

	bool ChainsLongerThanOne() const {
		return SljitHashJoinProbeLayoutChainsLongerThanOne(layout_kind);
	}

	bool UsesDictionaryEmission() const {
		return SljitHashJoinProbeLayoutUsesDictionaryEmission(layout_kind);
	}

	bool PreloadsAuxNextPointers() const {
		return ChainsLongerThanOne() && UsesDictionaryEmission();
	}

	bool UsesBloomFilter() const {
		return uses_bloom_filter;
	}

	bool EmitsMarkSelection() const {
		return SljitHashJoinEmitsMarkSelection(mark_selection_mode);
	}

	bool EmitsMarkMatchesAsSelection() const {
		return mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES;
	}

	bool EmitsMarkNonMatchesAsSelection() const {
		return mark_selection_mode == SljitHashJoinMarkSelectionMode::NON_MATCHES;
	}

	bool SpecializesNoChainLayout() const {
		return layout_kind == SljitHashJoinProbeLayoutKind::NO_CHAIN ||
		       layout_kind == SljitHashJoinProbeLayoutKind::NO_CHAIN_SALT;
	}
};

//! Controls which transient selections a perfect-hash probe materializes. The
//! fully elided form is only valid when its caller can derive the build index
//! from a proof-backed identity probe input.
struct SljitPerfectHashJoinProbeCodegenConfig {
	bool emit_match_selection = true;
	bool emit_build_selection = true;
};

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitRegularHashJoinProbe(const SljitNativeHashJoinProbePlan &plan,
                               SljitNativeRegularHashJoinProbeFunction &function, string &error,
                               const SljitHashJoinProbeCodegenConfig &config = {});
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNestedLoopJoinProbe(const SljitNativeNestedLoopJoinProbePlan &plan,
                                                                    SljitNativeNestedLoopJoinProbeFunction &function,
                                                                    string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitPerfectHashJoinProbe(const SljitNativeHashJoinProbePlan &plan,
                               SljitNativePerfectHashJoinProbeFunction &function, string &error,
                               const SljitPerfectHashJoinProbeCodegenConfig &config = {});

} // namespace duckdb

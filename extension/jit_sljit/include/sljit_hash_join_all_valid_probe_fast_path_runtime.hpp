//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_fast_path_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_facts.hpp"
#include "sljit_hash_join_runtime.hpp"

#include <array>

namespace duckdb {

using SljitAllValidHashJoinProbeExecutor = bool (*)(const SljitNativeHashJoinProbePlan &plan,
                                                    SljitNativeRegularHashJoinProbeInput &input,
                                                    const SljitAllValidHashJoinProbeFacts &facts);
using SljitAllValidHashJoinMarkSelectionProbeExecutor = bool (*)(const SljitNativeHashJoinProbePlan &plan,
                                                                 SljitNativeRegularHashJoinProbeInput &input,
                                                                 const SljitAllValidHashJoinProbeFacts &facts,
                                                                 SljitHashJoinMarkSelectionMode mark_selection_mode);

struct SljitAllValidHashJoinProbeFastPath {
	const char *flat_stage;
	const char *selected_stage;
	SljitAllValidHashJoinProbeExecutor execute;
};

struct SljitAllValidHashJoinMarkSelectionProbeFastPath {
	const char *flat_match_stage;
	const char *selected_match_stage;
	const char *flat_nonmatch_stage;
	const char *selected_nonmatch_stage;
	SljitAllValidHashJoinMarkSelectionProbeExecutor execute;
};

const char *SljitGeneratedAllValidRegularHashJoinProbeStage(bool selected);
const char *SljitGeneratedAllValidRegularHashJoinProbeStage(bool selected,
                                                            SljitHashJoinMarkSelectionMode mark_selection_mode);
const std::array<SljitAllValidHashJoinProbeFastPath, 5> &SljitAllValidHashJoinProbeFastPaths(bool selected);
const char *SljitAllValidHashJoinProbeFastPathStage(const SljitAllValidHashJoinProbeFastPath &fast_path, bool selected);
const std::array<SljitAllValidHashJoinMarkSelectionProbeFastPath, 6> &
SljitAllValidHashJoinMarkSelectionProbeFastPaths(bool selected);
const char *
SljitAllValidHashJoinMarkSelectionProbeFastPathStage(const SljitAllValidHashJoinMarkSelectionProbeFastPath &fast_path,
                                                     bool selected, SljitHashJoinMarkSelectionMode mark_selection_mode);

} // namespace duckdb

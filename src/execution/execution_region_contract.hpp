#pragma once

#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

ExecutionRegionSourceExecutionKind GetExecutionRegionCandidateSourceExecution(const ExecutionRegionCandidate &candidate,
                                                                              const ExecutionRegionNode &node);
bool ExecutionRegionCandidateUsesScanFilters(const ExecutionRegionCandidate &candidate,
                                             const ExecutionRegionNode &node);
ExecutionRegionContract BuildExecutionRegionContract(const ExecutionRegionIR &region_ir,
                                                     const ExecutionRegionCandidate &candidate,
                                                     ExecutionRegionIRMode mode);
idx_t CountExecutionRegionMissingOperatorContracts(const ExecutionRegionStagePlan &stage_plan);

} // namespace duckdb

#pragma once

#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

void FinalizeExecutionRegionSourceInfo(ExecutionRegionSourceInfo &source, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionOperatorInfo(ExecutionRegionOperatorInfo &operator_info, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionSinkInfo(ExecutionRegionSinkInfo &sink, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionCandidateTraits(ExecutionRegionCandidateTraits &traits, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionStagePlan(ExecutionRegionStagePlan &plan, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionCandidate(ExecutionRegionCandidate &candidate, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionIR(ExecutionRegionIR &region_ir, ExecutionRegionIRMode mode);

string DescribeExecutionRegionCandidateSpan(idx_t first_node, idx_t node_count, idx_t start_operator_index,
                                            idx_t end_operator_index,
                                            ExecutionRegionSourceExecutionKind source_execution);

} // namespace duckdb

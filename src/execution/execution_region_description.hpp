#pragma once

#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

void FinalizeExecutionRegionSourceInfo(ExecutionRegionSourceInfo &source, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionOperatorInfo(ExecutionRegionOperatorInfo &operator_info, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionSinkInfo(ExecutionRegionSinkInfo &sink, ExecutionRegionIRMode mode);
void FinalizeExecutionRegionStagePlan(ExecutionRegionStagePlan &plan, const string &candidate_shape,
                                      ExecutionRegionIRMode mode);
void FinalizeExecutionRegionIR(ExecutionRegionIR &region_ir, ExecutionRegionIRMode mode);

} // namespace duckdb

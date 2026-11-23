//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_ungrouped_shared_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_internal.hpp"

#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

void EmitLoadUngroupedAggregatePointer(struct sljit_compiler *compiler, sljit_sw pointer_array_offset, idx_t lane_idx,
                                       sljit_s32 target_reg);
void EmitUngroupedAggregateAddRowCount(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 count_reg);
void EmitUngroupedAggregateCommitCount(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 count_reg);
void EmitUngroupedAggregateCommitCount(struct sljit_compiler *compiler, idx_t lane_idx);
void EmitUngroupedAggregateCommitSumInt64(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset,
                                          sljit_sw saw_value_offset, sljit_s32 count_reg);
void EmitUngroupedAggregateCommitSumInt64(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset,
                                          sljit_sw saw_value_offset);
//! A DOUBLE sum's running value starts from the state, so the chunk's rows add to it in source order as DuckDB's sum
//! adds them
void EmitUngroupedAggregateLoadDoubleSum(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset);
void EmitUngroupedAggregateCommitSumDouble(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset,
                                           sljit_sw saw_value_offset, sljit_s32 count_reg);
//! Adds the value in value_reg (a DOUBLE's bits for a DOUBLE sum) to the running sum and marks it seen
void EmitUngroupedAggregateAccumulate(struct sljit_compiler *compiler, AggregatePrimitiveUpdateKind kind,
                                      sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                      sljit_sw saw_value_offset, sljit_s32 value_reg);
void EmitUngroupedAggregateAccumulateHugeintInt64Regs(struct sljit_compiler *compiler, sljit_s32 lower_reg,
                                                      sljit_s32 upper_reg, sljit_s32 value_reg);
void EmitUngroupedAggregateCommitSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                            sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                            sljit_sw saw_value_offset, sljit_s32 count_reg);
void EmitUngroupedAggregateCommitSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                            sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                            sljit_sw saw_value_offset);

} // namespace duckdb

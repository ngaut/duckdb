//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_input_complementary_sum_api.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class DataChunk;
class ExecutionOperatorRuntime;
class ExecutionRegionRuntime;
class SelectionVector;
class Vector;
struct SljitDirectJoinOutputAggregateStrategy;
struct SljitExecutableRegionOp;
struct SljitRegionExecutionScratch;

//! Batch-level boundary for the typed join-input complementary accumulator.
//! The implementation owns physical-type dispatch and calls this API at most
//! once per vector batch; no indirect dispatch enters its row loops.
bool SljitTryExecuteJoinInputRowPointerComplementarySumUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t hash_join_idx, SljitExecutableRegionOp &aggregate_op, SljitDirectJoinOutputAggregateStrategy &strategy,
    DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers, idx_t count,
    bool source_key0_int64_to_int32_unchecked, optional_ptr<string> failure_reason = nullptr);

//! Flushes the operator-lifetime complementary accumulator through the same
//! typed implementation owner.
bool SljitFlushJoinInputComplementarySumAccumulator(ExecutionRegionRuntime &runtime,
                                                    SljitExecutableRegionOp &aggregate_op,
                                                    SljitDirectJoinOutputAggregateStrategy &strategy);

} // namespace duckdb

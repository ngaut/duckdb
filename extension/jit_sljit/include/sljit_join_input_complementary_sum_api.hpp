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

//! Flushes the operator-lifetime complementary accumulator through the same
//! typed implementation owner.
bool SljitFlushJoinInputComplementarySumAccumulator(ExecutionRegionRuntime &runtime,
                                                    SljitExecutableRegionOp &aggregate_op,
                                                    SljitDirectJoinOutputAggregateStrategy &strategy);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_contract.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_state.hpp"

namespace duckdb {

SljitFullPipelineRecipe
SljitFinalizeFullPipelinePrimitiveRecipe(const vector<SljitExecutableRegionOp> &ops,
                                         SljitFullPipelinePrimitiveSequence primitive_sequence,
                                         SljitHashJoinDirectAggregateConsumerContract direct_aggregate_consumer = {});

//! Whether DuckDB's operators can take the generated steps' output at op `tail_start_idx`: the ops before it compute
//! exactly the pipeline's operators before the one it starts at, so their output is that operator's input
bool SljitNativeTailCanConsumeTail(const vector<SljitExecutableRegionOp> &ops, idx_t tail_start_idx);
//! The pipeline operator DuckDB's operators take the rows over at; INVALID_INDEX for the sink itself
idx_t SljitNativeTailOperatorIndex(const vector<SljitExecutableRegionOp> &ops, idx_t tail_start_idx);
//! Whether running the region executes generated code: a recipe step runs an op that carries code, or DuckDB's scan
//! runs the region's compiled filters. A region that does neither is DuckDB's pipeline and runs as one
bool SljitRegionRunsGeneratedCode(const vector<SljitExecutableRegionOp> &ops, const SljitFullPipelineRecipe &recipe,
                                  ExecutionRegionScanFilterMode scan_filter_mode,
                                  const vector<SljitExecutableScanFilter> &scan_filters);

} // namespace duckdb

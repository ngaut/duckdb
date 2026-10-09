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

bool SljitNativeTailCanConsumeTail(const vector<SljitExecutableRegionOp> &ops, idx_t tail_start_idx);

} // namespace duckdb

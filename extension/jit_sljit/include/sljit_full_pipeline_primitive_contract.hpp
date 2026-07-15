//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_contract.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"

namespace duckdb {

bool SljitFullPipelineSourceFetchOwnsSinkAdvance(const SljitFullPipelinePrimitiveSequence &primitive_sequence);

bool SljitFullPipelineSourceFetchNeedsPartitionPreservingChunks(
    const SljitFullPipelinePrimitiveSequence &primitive_sequence);

bool SljitFullPipelineIsSelectedHashJoinSinkSequence(const SljitFullPipelinePrimitiveSequence &primitive_sequence);

bool SljitFullPipelineHasDirectSourceHashBuild(const SljitFullPipelinePrimitiveSequence &primitive_sequence);

bool SljitFullPipelineHasExactFilterProbeHashBuild(const vector<SljitExecutableRegionOp> &ops,
                                                   const SljitFullPipelinePrimitiveSequence &primitive_sequence);

bool SljitFullPipelineFilterHasFusedOwner(const vector<SljitExecutableRegionOp> &ops,
                                          const SljitFullPipelinePrimitiveSequence &primitive_sequence,
                                          idx_t filter_idx);

bool SljitNativeTailCanConsumeTail(const vector<SljitExecutableRegionOp> &ops, idx_t tail_start_idx);

bool SljitFullPipelinePrimitiveSequenceIsExecutable(const vector<SljitExecutableRegionOp> &ops,
                                                    const SljitFullPipelinePrimitiveSequence &sequence);

const SljitFullPipelinePrimitiveStep &
SljitFullPipelinePrimitiveSequenceTerminalStep(const SljitFullPipelinePrimitiveSequence &sequence);

} // namespace duckdb

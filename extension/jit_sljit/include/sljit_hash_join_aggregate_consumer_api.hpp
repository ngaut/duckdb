//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_aggregate_consumer_api.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_consumer_result.hpp"

#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class DataChunk;
class ExecutionOperatorRuntime;
class ExecutionRegionRuntime;
class SljitNativeRegionKernel;
struct SljitDirectJoinOutputAggregateStrategy;
struct SljitExecutableRegionOp;
struct SljitHashJoinProbeInputFilterCache;
struct SljitHashJoinProbeSelectionPrimitive;
struct SljitPostJoinProjectionStrategy;
struct SljitRegionExecutionScratch;
class SljitSharedPerfectHashPredicateClassificationCache;
template <class OWNER>
struct SljitRecordedHashJoinProbeCallback;

using SljitNativeRegionHashJoinProbeExecutor = SljitRecordedHashJoinProbeCallback<SljitNativeRegionKernel>;

//! Batch-level direct-consumer boundary. The concrete probe executor keeps
//! codegen calls statically bound; the implementation owner contains all
//! complementary-accumulator physical-type dispatch.
SljitHashJoinAggregateConsumerResult SljitTryExecuteHashJoinAggregateConsumer(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, const SljitHashJoinProbeSelectionPrimitive &probe_primitive,
    SljitExecutableRegionOp &hash_join_op, SljitDirectJoinOutputAggregateStrategy &strategy, DataChunk &join_input,
    SljitPostJoinProjectionStrategy &post_join_projection, optional_ptr<const vector<idx_t>> output_column_map,
    idx_t output_projection_idx, idx_t probe_input_filter_idx,
    SljitHashJoinProbeInputFilterCache &probe_input_filter_cache,
    SljitSharedPerfectHashPredicateClassificationCache &shared_predicate_classification,
    SljitNativeRegionHashJoinProbeExecutor &execute_hash_join_probe);

} // namespace duckdb

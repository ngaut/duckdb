//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_runtime_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_distinct_key_sink_runtime.hpp"
#include "sljit_grouped_direct_aggregate_update_runtime_state.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_grouped_count_star_update_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

struct SljitGroupedAggregateUpdateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE ||
		    primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return direct_payload_update.Prepare(ops, primitive);
		}
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK) {
			return distinct_key_sink.Prepare(primitive);
		}
		return count_star_preaggregation.Prepare(runtime, ops, scratch, primitive);
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (input.count == 0) {
			return false;
		}
		switch (primitive.strategy) {
		case SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION:
			return count_star_preaggregation.Execute(runtime, ops, scratch, primitive, input, processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE:
		case SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
			return direct_payload_update.Execute(runtime, result, ops, scratch, primitive, input, processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK:
			return distinct_key_sink.Execute(runtime, result, ops, scratch, primitive, input, processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			break;
		}
		throw InternalException("SLJIT grouped aggregate update primitive has an unknown strategy");
	}

	bool Flush(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE ||
		    primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return direct_payload_update.Flush(runtime, ops, scratch, primitive);
		}
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK) {
			return false;
		}
		return count_star_preaggregation.Flush(runtime, ops, scratch, primitive);
	}

private:
	SljitGroupedCountStarPreaggregationRuntimeState count_star_preaggregation;
	SljitGroupedDirectPrimitivePayloadUpdateRuntimeState direct_payload_update;
	SljitGroupedDistinctKeySinkRuntimeState distinct_key_sink;
};

} // namespace duckdb

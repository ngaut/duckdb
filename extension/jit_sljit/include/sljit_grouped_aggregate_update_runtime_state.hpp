//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_runtime_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
		return count_star_preaggregation.Prepare(runtime, ops, scratch, primitive);
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input) {
		if (input.count == 0) {
			return false;
		}
		switch (primitive.strategy) {
		case SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION:
			return count_star_preaggregation.Execute(runtime, ops, scratch, primitive, input);
		case SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE:
		case SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
			return direct_payload_update.Execute(runtime, result, ops, scratch, primitive, input);
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
		return count_star_preaggregation.Flush(runtime, ops, scratch, primitive);
	}

	void DecideRunStrategy(vector<SljitExecutableRegionOp> &ops, const SljitGroupedAggregateUpdatePrimitive &primitive,
	                       const SljitRuntimeBatchView &input) {
		direct_payload_update.DecideRunStrategy(ops, primitive, input);
	}

	bool DelegatesToNativePipeline(const SljitGroupedAggregateUpdatePrimitive &primitive) const {
		return direct_payload_update.DelegatesToNativePipeline(primitive);
	}

	void BeginNativePipelineDelegation(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                   SljitRegionExecutionScratch &scratch,
	                                   const SljitGroupedAggregateUpdatePrimitive &primitive) {
		direct_payload_update.BeginNativePipelineDelegation(runtime, ops, scratch, primitive);
	}

private:
	SljitGroupedCountStarPreaggregationRuntimeState count_star_preaggregation;
	SljitGroupedDirectPrimitivePayloadUpdateRuntimeState direct_payload_update;
};

} // namespace duckdb

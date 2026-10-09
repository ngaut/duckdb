//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projected_aggregate_input_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"

namespace duckdb {

struct SljitPreparedProjectedAggregateInput {
	DataChunk *source_chunk = nullptr;
	const SelectionVector *selection = nullptr;
	idx_t count = 0;
};

static bool SljitPrepareProjectedAggregateInput(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                                                SljitRegionExecutionScratch &scratch,
                                                const SljitRuntimeBatchView &input,
                                                const SljitExecutableRegionOp &projection,
                                                SljitDataChunkBatch &selected_hash_join_input,
                                                const char *materialized_context,
                                                SljitPreparedProjectedAggregateInput &prepared) {
	prepared = SljitPreparedProjectedAggregateInput();
	prepared.selection = input.selection;
	prepared.count = input.count;
	if (input.HasHashJoinSelection()) {
		if (!SljitTryPrepareSelectedHashJoinProjectionInput(runtime, scratch, ops, projection, input,
		                                                    selected_hash_join_input, prepared.source_chunk)) {
			return false;
		}
		prepared.selection = nullptr;
		prepared.count = prepared.source_chunk->size();
		return true;
	}
	prepared.source_chunk = &SljitBindRuntimeBatchInput(input, materialized_context);
	return true;
}

} // namespace duckdb

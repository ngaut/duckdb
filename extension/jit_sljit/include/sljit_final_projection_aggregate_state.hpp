//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_final_projection_aggregate_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_region_adapter_scratch.hpp"

namespace duckdb {

struct SljitFinalProjectionAggregateBridge {
	SljitFinalProjectionAggregateBridge() : group_key_hashes(LogicalType::HASH) {
	}

	SljitDataChunkBatch group_key_batch;
	Vector group_key_hashes;
	SljitDeferredBuildState split_payload_descriptor;
	bool split_payload_uses_fused_update = false;
	SljitJoinProjectionAggregateDescriptor row_pointer_aggregate;
	vector<LogicalType> group_key_types;
	vector<idx_t> group_projection_indices;
	vector<idx_t> payload_source_indices;
	vector<SljitExpressionAdapterScratch> group_projection_scratch;
};

} // namespace duckdb
